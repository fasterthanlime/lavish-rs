use super::facts::Mapping;
use super::{Atom, Error, Message, PendingRequests};

use std::any::Any;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
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

pub trait Handler<CL, M, P, NP, R>: Sync + Send
where
    CL: Clone,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    fn handle(&self, client: Caller<M, P, NP, R>, params: P) -> Result<R, Error>;
    fn make_client(client: Caller<M, P, NP, R>) -> CL;
}

#[derive(Debug)]
pub enum SinkValue<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    Shutdown,
    Message(Message<M, P, NP, R>),
}

pub struct Caller<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    conn_ref: Option<Arc<ConnRef>>,
    shutdown_handle: Box<Conn>,
    queue: Arc<Mutex<Queue<M, P, NP, R>>>,
    sink: mpsc::Sender<SinkValue<M, P, NP, R>>,
}

impl<M, P, NP, R> Drop for Caller<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    fn drop(&mut self) {
        debug!("Caller: dropping!");
    }
}

impl<M, P, NP, R> Clone for Caller<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    fn clone(&self) -> Self {
        Caller {
            conn_ref: self.conn_ref.clone(),
            queue: self.queue.clone(),
            sink: self.sink.clone(),
            shutdown_handle: self.shutdown_handle.try_clone().unwrap(),
        }
    }
}

impl<M, P, NP, R> Caller<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    pub fn call_raw(&self, params: P) -> Result<Message<M, P, NP, R>, Error> {
        let id = {
            let mut queue = self.queue.lock()?;
            queue.next_id()
        };

        let method = params.method();
        let m = Message::Request {
            id,
            params,
            phantom: PhantomData,
        };

        let (tx, rx) = mpsc::channel::<Message<M, P, NP, R>>();
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
        self.shutdown_handle.shutdown(Shutdown::Both).ok();
    }
}

pub fn default_timeout() -> Duration {
    Duration::from_secs(2)
}

// Connect to an address over TCP, then spawn a new RPC
// system with the given handler.
pub fn connect<M, AH, A, H, CL, P, NP, R>(
    handler: AH,
    addr: A,
) -> Result<Runtime<CL, H, M, P, NP, R>, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, M, P, NP, R> + 'static,
    CL: Clone,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    connect_timeout(handler, addr, Some(default_timeout()))
}

pub fn connect_timeout<M, AH, A, H, CL, P, NP, R>(
    handler: AH,
    addr: A,
    timeout: Option<Duration>,
) -> Result<Runtime<CL, H, M, P, NP, R>, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, M, P, NP, R> + 'static,
    CL: Clone,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    let conn = match timeout {
        Some(timeout) => {
            let addr = addr.to_socket_addrs()?.next().unwrap();
            TcpStream::connect_timeout(&addr, timeout)
        }
        None => TcpStream::connect(addr),
    }?;
    conn.set_nodelay(true)?;
    spawn(handler, Box::new(conn))
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

pub fn serve<M, AH, A, H, CL, P, NP, R>(handler: AH, addr: A) -> Result<Server, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, M, P, NP, R> + 'static,
    CL: Clone,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    serve_max_conns(handler, addr, None)
}

pub fn serve_once<M, AH, A, H, CL, P, NP, R>(handler: AH, addr: A) -> Result<Server, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, M, P, NP, R> + 'static,
    CL: Clone,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    serve_max_conns(handler, addr, Some(1))
}

pub fn serve_max_conns<M, AH, A, H, CL, P, NP, R>(
    handler: AH,
    addr: A,
    max_conns: Option<usize>,
) -> Result<Server, Error>
where
    AH: Into<Arc<H>>,
    A: ToSocketAddrs,
    H: Handler<CL, M, P, NP, R> + 'static,
    CL: Clone,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    let handler = handler.into();
    let listener = TcpListener::bind(addr)?;
    let local_addr = listener.local_addr()?;

    let join_handle = std::thread::spawn(move || {
        let mut conn_number = 0;
        let mut incoming = listener.incoming();
        let mut runtimes = Vec::<Runtime<CL, H, M, P, NP, R>>::new();

        while let Some(conn) = incoming.next() {
            let conn = conn.unwrap();
            conn.set_nodelay(true).unwrap();
            let handler = handler.clone();
            // oh poor rustc you needed a little push there
            let runtime = spawn::<CL, M, Arc<H>, H, P, NP, R>(handler, Box::new(conn)).unwrap();

            conn_number += 1;
            if let Some(max_conns) = max_conns {
                // only collect runtimes if we have a maximum number of
                // connections - otherwise, it'd just grow unbounded.
                runtimes.push(runtime);
                if conn_number >= max_conns {
                    break;
                }
            }
        }

        for mut r in runtimes.drain(..) {
            r.join().unwrap();
        }
    });
    Ok(Server {
        join_handle,
        local_addr,
    })
}

pub struct Runtime<CL, H, M, P, NP, R>
where
    CL: Clone,
    M: Mapping,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    H: Handler<CL, M, P, NP, R> + 'static,
{
    caller: Caller<M, P, NP, R>,
    encode_handle: Option<JoinHandle<()>>,
    decode_handle: Option<JoinHandle<()>>,
    shutdown_handle: Box<Conn>,
    phantom: PhantomData<(CL, H)>,
}

impl<CL, H, M, P, NP, R> Runtime<CL, H, M, P, NP, R>
where
    CL: Clone,
    M: Mapping,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    H: Handler<CL, M, P, NP, R> + 'static,
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
        H::make_client(self.caller.clone())
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

pub fn spawn<CL, M, AH, H, P, NP, R>(
    handler: AH,
    conn: Box<Conn>,
) -> Result<Runtime<CL, H, M, P, NP, R>, Error>
where
    CL: Clone,
    AH: Into<Arc<H>>,
    H: Handler<CL, M, P, NP, R> + 'static,
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
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
    let mut decoder = Decoder::new(read);
    // FIXME: default is wrong here, obviously.
    let mut encoder = Encoder::new(write, M::default());
    let (tx, rx) = mpsc::channel();
    let runtime_tx = tx.clone();

    let runtime_caller = Caller::<M, P, NP, R> {
        conn_ref: Some(conn_ref.clone()),
        shutdown_handle: decode_shutdown_handle.try_clone()?,
        queue: queue.clone(),
        sink: tx.clone(),
    };

    let internal_caller = Caller::<M, P, NP, R> {
        conn_ref: None,
        shutdown_handle: decode_shutdown_handle.try_clone()?,
        queue: queue.clone(),
        sink: tx.clone(),
    };

    let encode_queue = queue.clone();
    let encode_handle = std::thread::spawn(move || {
        'relay: loop {
            match rx.recv() {
                Ok(val) => match val {
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
                },
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
                phantom: PhantomData,
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
                        let caller = internal_caller.clone();

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
        caller: runtime_caller,
        shutdown_handle,
        encode_handle: Some(encode_handle),
        decode_handle: Some(decode_handle),
        phantom: PhantomData,
    })
}

fn handle_message<M, P, NP, R, H, CL>(
    inbound: Message<M, P, NP, R>,
    handler: Arc<H>,
    caller: Caller<M, P, NP, R>,
) -> Result<(), Error>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    CL: Clone,
    H: Handler<CL, M, P, NP, R>,
    M: Mapping,
{
    match inbound {
        Message::Request { id, params, .. } => {
            let m = match handler.handle(caller.clone(), params) {
                Ok(results) => Message::Response::<M, P, NP, R> {
                    id,
                    results: Some(results),
                    error: None,
                    phantom: PhantomData,
                },
                Err(error) => Message::Response::<M, P, NP, R> {
                    id,
                    results: None,
                    error: Some(format!("internal error: {:#?}", error)),
                    phantom: PhantomData,
                },
            };
            caller.sink.send(SinkValue::Message(m))?;
        }
        Message::Response {
            id, error, results, ..
        } => {
            if let Some(in_flight) = {
                let mut queue = caller.queue.lock()?;
                queue.in_flight_requests.remove(&id)
            } {
                in_flight.tx.send(Message::Response {
                    id,
                    error,
                    results,
                    phantom: PhantomData,
                })?;
            }
        }
        Message::Notification { .. } => unimplemented!(),
    };
    Ok(())
}

struct InFlightRequest<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    method: &'static str,
    tx: mpsc::Sender<Message<M, P, NP, R>>,
}

pub struct Queue<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    id: u32,
    in_flight_requests: HashMap<u32, InFlightRequest<M, P, NP, R>>,
}

impl<M, P, NP, R> Queue<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
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

impl<M, P, NP, R> PendingRequests for Queue<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    fn get_pending(&self, id: u32) -> Option<&'static str> {
        self.in_flight_requests.get(&id).map(|req| req.method)
    }
}
