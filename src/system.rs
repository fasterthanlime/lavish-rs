use super::{Atom, Error, Message, PendingRequests};

use std::any::Any;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use log::*;

mod codec;
use codec::{Decoder, Encoder};

pub trait Conn: Read + Write + Send + Sized + 'static {
    fn try_clone(&self) -> io::Result<Self>;
    fn shutdown(&self, how: Shutdown) -> io::Result<()>;
}

impl Conn for std::net::TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
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
    queue: Arc<Mutex<Queue<P, NP, R>>>,
    sink: mpsc::Sender<SinkValue<P, NP, R>>,
}

impl<P, NP, R> Caller<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn clone(&self) -> Self {
        Caller {
            queue: self.queue.clone(),
            sink: self.sink.clone(),
        }
    }

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
}

pub fn default_timeout() -> Duration {
    Duration::from_secs(2)
}

// Connect to a TCP address, then spawn a new RPC
// system with the given handler
pub fn connect_tcp<AH, H, CL, P, NP, R>(
    handler: AH,
    addr: &SocketAddr,
) -> Result<Runtime<TcpStream, CL>, Error>
where
    AH: Into<Arc<H>>,
    H: Handler<CL, P, NP, R> + 'static,
    CL: Clone,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let conn = TcpStream::connect_timeout(addr, default_timeout())?;
    return spawn(handler, conn);
}

pub struct Runtime<C, CL>
where
    C: Conn,
    CL: Clone,
{
    client_template: CL,
    encode_handle: Option<JoinHandle<()>>,
    decode_handle: Option<JoinHandle<()>>,
    shutdown_handle: C,
}

impl<C, CL> Runtime<C, CL>
where
    C: Conn,
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
        debug!("Runtime: shutting down conn");
        self.shutdown_handle.shutdown(Shutdown::Both).ok();
        debug!("Runtime: conn was shut down (both sides)");
    }
}

impl<C, CL> Drop for Runtime<C, CL>
where
    C: Conn,
    CL: Clone,
{
    fn drop(&mut self) {
        debug!("Runtime dropped!");
        self.shutdown();
    }
}

pub fn spawn<C, CL, AH, H, P, NP, R>(handler: AH, conn: C) -> Result<Runtime<C, CL>, Error>
where
    C: Conn,
    CL: Clone,
    AH: Into<Arc<H>>,
    H: Handler<CL, P, NP, R> + 'static,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let handler = handler.into();
    let queue = Arc::new(Mutex::new(Queue::new()));

    let shutdown_handle = conn.try_clone()?;
    let encoder_shutdown_handle = conn.try_clone()?;
    let decoder_shutdown_handle = conn.try_clone()?;

    let write = conn.try_clone()?;
    let read = conn;
    let mut decoder = Decoder::new(read, queue.clone());
    let mut encoder = Encoder::new(write);
    let (tx, rx) = mpsc::channel();
    let runtime_tx = tx.clone();

    let client = Caller::<P, NP, R> {
        queue: queue.clone(),
        sink: tx,
    };

    let client_template = H::make_client(client.clone());

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
        encoder_shutdown_handle.shutdown(Shutdown::Both).ok();

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
                        let client = client.clone();

                        std::thread::spawn(move || {
                            let res = handle_message(message, handler, client);
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
        decoder_shutdown_handle.shutdown(Shutdown::Both).ok();

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
    client: Caller<P, NP, R>,
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
            let m = match handler.handle(client.clone(), params) {
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
            client.sink.send(SinkValue::Message(m))?;
        }
        Message::Response { id, error, results } => {
            if let Some(in_flight) = {
                let mut queue = client.queue.lock()?;
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
