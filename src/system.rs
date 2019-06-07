use super::{Atom, Error, Message, PendingRequests};

use std::any::Any;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use log::*;

mod codec;
use codec::{Decoder, Encoder};

pub trait Conn: Read + Write + Send + Sync + 'static {
    fn try_clone(&self) -> io::Result<Box<Conn>>;
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl Conn for std::net::TcpStream {
    fn try_clone(&self) -> io::Result<Box<Conn>> {
        let other = self.try_clone()?;
        Ok(Box::new(other))
    }

    fn shutdown(&self, how: Shutdown) -> io::Result<()> {
        self.shutdown(how)
    }
}

pub trait Handler<CL, P, NP, R>: Sync + Send
where
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn handle(&self, client: Caller<P, NP, R>, params: P) -> Result<R, Error>;
    fn make_client(client: Caller<P, NP, R>) -> CL;
}

#[derive(Debug)]
pub enum SinkValue<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    Shutdown,
    Message(Message<P, NP, R>),
}

pub struct Caller<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    conn_ref: Arc<ConnRef>,
    queue: Arc<Mutex<Queue<P, NP, R>>>,
    sink: mpsc::Sender<SinkValue<P, NP, R>>,
}

impl<P, NP, R> Clone for Caller<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn clone(&self) -> Self {
        Caller {
            conn_ref: self.conn_ref.clone(),
            queue: self.queue.clone(),
            sink: self.sink.clone(),
        }
    }
}

impl<P, NP, R> Caller<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    pub fn call_raw(&self, params: P) -> Result<Message<P, NP, R>, Error> {
        let id = {
            let mut queue = self.queue.lock()?;
            queue.next_id()
        };

        let method = params.method();
        let m = Message::Request { id, params };

        let (tx, rx) = mpsc::channel::<Message<P, NP, R>>();
        let in_flight = InFlightRequest { method, tx };
        {
            let mut queue = self.queue.lock()?;
            queue.in_flight_requests.insert(id, in_flight);
        }

        {
            let sink = self.sink.clone();
            sink.send(SinkValue::Message(m))?;
        }
        Ok(rx.recv()?)
    }

    #[allow(clippy::needless_lifetimes)]
    pub fn call<D, RR>(&self, params: P, downgrade: D) -> Result<RR, Error>
    where
        D: Fn(R) -> Option<RR>,
    {
        match self.call_raw(params) {
            Ok(m) => match m {
                Message::Response { results, error, .. } => {
                    if let Some(error) = error {
                        Err(Error::RemoteError(error))
                    } else if let Some(results) = results {
                        downgrade(results).ok_or_else(|| Error::WrongResults)
                    } else {
                        Err(Error::MissingResults)
                    }
                }
                _ => Err(Error::WrongMessageType),
            },
            Err(msg) => Err(Error::TransportError(format!("{:#?}", msg))),
        }
    }

    pub fn shutdown_runtime(&self) {
        self.conn_ref.shutdown();
    }
}

pub fn default_timeout() -> Duration {
    Duration::from_secs(2)
}

// Connect to an address over TCP, then spawn a new RPC
// system with the given handler.
pub fn connect<AH, A, H, CL, P, NP, R>(handler: AH, addr: A) -> Result<Runtime<CL>, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, P, NP, R> + 'static,
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    connect_timeout(handler, addr, Some(default_timeout()))
}

pub fn connect_timeout<AH, A, H, CL, P, NP, R>(
    handler: AH,
    addr: A,
    timeout: Option<Duration>,
) -> Result<Runtime<CL>, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, P, NP, R> + 'static,
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let conn = match timeout {
        Some(timeout) => {
            let addr = addr.to_socket_addrs()?.next().unwrap();
            TcpStream::connect_timeout(&addr, timeout)
        }
        None => TcpStream::connect(addr),
    }?;
    conn.set_nodelay(true)?;
    return spawn(handler, Box::new(conn));
}

pub struct Server {
    join_handle: JoinHandle<()>,
    local_addr: SocketAddr,
}

impl Server {
    pub fn join(self) -> Result<(), Box<dyn Any + Send + 'static>> {
        self.join_handle.join()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

pub fn serve<AH, A, H, CL, P, NP, R>(handler: AH, addr: A) -> Result<Server, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, P, NP, R> + 'static,
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    serve_max_conns(handler, addr, None)
}

pub fn serve_once<AH, A, H, CL, P, NP, R>(handler: AH, addr: A) -> Result<Server, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, P, NP, R> + 'static,
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    serve_max_conns(handler, addr, Some(1))
}

pub fn serve_max_conns<AH, A, H, CL, P, NP, R>(
    handler: AH,
    addr: A,
    max_conns: Option<usize>,
) -> Result<Server, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, P, NP, R> + 'static,
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let handler = handler.into();
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    let join_handle = std::thread::spawn(move || {
        let mut conn_number = 0;
        let mut incoming = listener.incoming();
        while let Some(conn) = incoming.next() {
            let conn = conn.unwrap();
            conn.set_nodelay(true).unwrap();
            let handler = handler.clone();
            // oh poor rustc you needed a little push there
            spawn::<CL, Arc<H>, H, P, NP, R>(handler, Box::new(conn)).unwrap();

            conn_number += 1;
            if let Some(max_conns) = max_conns {
                if conn_number >= max_conns {
                    return;
                }
            }
        }
    });
    Ok(Server {
        join_handle,
        local_addr,
    })
}

pub struct Runtime<CL>
where
    CL: Clone,
{
    client_template: CL,
    encode_handle: Option<JoinHandle<()>>,
    decode_handle: Option<JoinHandle<()>>,
    shutdown_handle: Box<Conn>,
}

impl<CL> Runtime<CL>
where
    CL: Clone,
{
    pub fn join(&mut self) -> Result<(), Box<dyn Any + Send>> {
        if let Some(h) = self.encode_handle.take() {
            h.join()?;
        }
        if let Some(h) = self.decode_handle.take() {
            h.join()?;
        }
        Ok(())
    }

    pub fn client(&self) -> CL {
        self.client_template.clone()
    }

    pub fn shutdown(&self) {
        debug!("Runtime: explicit shutdown requested");
        self.shutdown_handle.shutdown(Shutdown::Both).ok();
    }
}

struct ConnRef {
    conn: Box<Conn>,
}

impl ConnRef {
    fn shutdown(&self) {
        debug!("ConnRef: dropping, shutting down connection");
        self.conn.shutdown(Shutdown::Both).ok();
    }
}

impl Drop for ConnRef {
    fn drop(&mut self) {
        self.shutdown();
    }
}

pub fn spawn<CL, AH, H, P, NP, R>(handler: AH, conn: Box<Conn>) -> Result<Runtime<CL>, Error>
where
    CL: Clone,
    AH: Into<Arc<H>>,
    H: Handler<CL, P, NP, R> + 'static,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let handler = handler.into();
    let queue = Arc::new(Mutex::new(Queue::new()));
    let conn_ref = Arc::new(ConnRef {
        conn: conn.try_clone()?,
    });

    let shutdown_handle = conn.try_clone()?;
    let encode_shutdown_handle = conn.try_clone()?;
    let decode_shutdown_handle = conn.try_clone()?;
    let write = conn.try_clone()?;
    let read = conn;
    let mut decoder = Decoder::new(read, queue.clone());
    let mut encoder = Encoder::new(write);
    let (tx, rx) = mpsc::channel();
    let runtime_tx = tx.clone();

    let caller = Caller::<P, NP, R> {
        conn_ref: conn_ref.clone(),
        queue: queue.clone(),
        sink: tx,
    };

    let client_template = H::make_client(caller.clone());

    let encode_queue = queue.clone();
    let encode_handle = std::thread::spawn(move || {
        'relay: loop {
            match rx.recv() {
                Ok(val) => {
                    debug!("Encoder thread: received {:#?}", val);
                    match val {
                        SinkValue::Message(m) => {
                            if let Err(e) = encoder.encode(m) {
                                debug!("Encoder thread: could not encode: {:#?}", e);
                                debug!("Encoder thread: breaking");
                                break 'relay;
                            }
                        }
                        SinkValue::Shutdown => {
                            debug!("Encoder thread: received shutdown");
                            break 'relay;
                        }
                    }
                }
                Err(e) => {
                    debug!("Encoder thread: could not receive");
                    debug!("Encoder thread: error was: {:#?}", e);
                    break 'relay;
                }
            }
        }

        debug!("Encoder thread watcher: shutting down conn");
        encode_shutdown_handle.shutdown(Shutdown::Both).ok();

        debug!("Encoder thread watcher: cancelling pending requests");
        let queue = encode_queue.lock().unwrap();
        for (k, v) in &queue.in_flight_requests {
            v.tx.send(Message::Response {
                id: *k,
                error: Some("Connection closed".into()),
                results: None,
            })
            .ok();
        }
        debug!("Encoder thread watcher: done");
    });

    let decode_handle = std::thread::spawn(move || {
        'relay: loop {
            match decoder.decode() {
                Err(e) => {
                    debug!("Decode thread: could not decode: {:#?}", e);
                    debug!("Decode thread: breaking");
                    break 'relay;
                }
                Ok(res) => match res {
                    Some(message) => {
                        let handler = handler.clone();
                        let caller = caller.clone();

                        std::thread::spawn(move || {
                            let res = handle_message(message, handler, caller);
                            if let Err(e) = res {
                                eprintln!("message stream error: {:#?}", e);
                            }
                        });
                    }
                    None => {
                        // Signals EOF, we're done here
                        debug!("Decode thread: received None value, end of stream");
                        debug!("Decode thread: breaking");
                        break 'relay;
                    }
                },
            }
        }

        debug!("Decoder thread watcher: sending shutdown to encode thread");
        runtime_tx.send(SinkValue::Shutdown).ok();

        debug!("Decoder thread watcher: shutting down conn");
        decode_shutdown_handle.shutdown(Shutdown::Both).ok();

        debug!("Decoder thread watcher: done");
    });

    debug!("Spawned runtime!");
    Ok(Runtime {
        client_template,
        shutdown_handle,
        encode_handle: Some(encode_handle),
        decode_handle: Some(decode_handle),
    })
}

fn handle_message<P, NP, R, H, CL>(
    inbound: Message<P, NP, R>,
    handler: Arc<H>,
    caller: Caller<P, NP, R>,
) -> Result<(), Error>
where
    P: Atom,
    NP: Atom,
    R: Atom,
    CL: Clone,
    H: Handler<CL, P, NP, R>,
{
    match inbound {
        Message::Request { id, params } => {
            let m = match handler.handle(caller.clone(), params) {
                Ok(results) => Message::Response::<P, NP, R> {
                    id,
                    results: Some(results),
                    error: None,
                },
                Err(error) => Message::Response::<P, NP, R> {
                    id,
                    results: None,
                    error: Some(format!("internal error: {:#?}", error)),
                },
            };
            caller.sink.send(SinkValue::Message(m))?;
        }
        Message::Response { id, error, results } => {
            if let Some(in_flight) = {
                let mut queue = caller.queue.lock()?;
                queue.in_flight_requests.remove(&id)
            } {
                in_flight
                    .tx
                    .send(Message::Response { id, error, results })?;
            }
        }
        Message::Notification { .. } => unimplemented!(),
    };
    Ok(())
}

struct InFlightRequest<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    method: &'static str,
    tx: mpsc::Sender<Message<P, NP, R>>,
}

pub struct Queue<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    id: u32,
    in_flight_requests: HashMap<u32, InFlightRequest<P, NP, R>>,
}

impl<P, NP, R> Queue<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn new() -> Self {
        Queue {
            id: 0,
            in_flight_requests: HashMap::new(),
        }
    }

    fn next_id(&mut self) -> u32 {
        let res = self.id;
        self.id += 1;
        res
    }
}

impl<P, NP, R> PendingRequests for Queue<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn get_pending(&self, id: u32) -> Option<&'static str> {
        self.in_flight_requests.get(&id).map(|req| req.method)
    }
}
