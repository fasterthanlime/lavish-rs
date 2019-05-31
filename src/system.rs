use super::{Atom, Error, Message, PendingRequests};

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::sync::{mpsc, Arc, Mutex};

mod codec;
use codec::{Decoder, Encoder};

pub trait Conn: Read + Write + Send + Sized + 'static {
    fn try_clone(&self) -> io::Result<Self>;
}

impl Conn for std::net::TcpStream {
    fn try_clone(&self) -> io::Result<Self> {
        self.try_clone()
    }
}

#[derive(Clone, Copy)]
pub struct Protocol<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    phantom: PhantomData<(P, NP, R)>,
}

impl<P, NP, R> Protocol<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            phantom: PhantomData,
        }
    }
}

pub trait Handler<P, NP, R>: Sync + Send
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn handle(&self, client: Client<P, NP, R>, params: P) -> Result<R, Error>;
}

impl<P, NP, R, F> Handler<P, NP, R> for F
where
    P: Atom,
    R: Atom,
    NP: Atom,
    F: (Fn(Client<P, NP, R>, P) -> Result<R, Error>) + Send + Sync,
{
    fn handle(&self, client: Client<P, NP, R>, params: P) -> Result<R, Error> {
        self(client, params)
    }
}

pub struct Client<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    queue: Arc<Mutex<Queue<P, NP, R>>>,
    sink: mpsc::Sender<Message<P, NP, R>>,
}

impl<P, NP, R> Client<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    fn clone(&self) -> Self {
        Client {
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
            sink.send(m)?;
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

pub fn connect<C, H, P, NP, R>(
    protocol: Protocol<P, NP, R>,
    handler: H,
    io: C,
) -> Result<Client<P, NP, R>, Error>
where
    C: Conn,
    H: Handler<P, NP, R> + 'static,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let queue = Arc::new(Mutex::new(Queue::new(protocol)));

    let write = io.try_clone()?;
    let read = io;
    let mut decoder = Decoder::new(read, queue.clone());
    let mut encoder = Encoder::new(write);
    let (tx, rx) = mpsc::channel();

    let client = Client::<P, NP, R> {
        queue: queue.clone(),
        sink: tx,
    };

    let ret = client.clone();

    // TODO: shutdown handling
    std::thread::spawn(move || {
        // FIXME: error handling
        loop {
            let m = rx.recv().unwrap();
            encoder.encode(m).unwrap();
        }
    });

    // TODO: shutdown handling
    std::thread::spawn(move || {
        let handler = Arc::new(handler);

        // FIXME: error handling
        loop {
            let m = decoder.decode().unwrap();
            let handler = handler.clone();
            let client = client.clone();

            std::thread::spawn(move || {
                // TODO: error handling
                let res = handle_message(m, handler, client);
                if let Err(e) = res {
                    eprintln!("message stream error: {:#?}", e);
                }
            });
        }
    });

    Ok(ret)
}

fn handle_message<P, NP, R, H>(
    inbound: Message<P, NP, R>,
    handler: Arc<H>,
    client: Client<P, NP, R>,
) -> Result<(), Error>
where
    P: Atom,
    NP: Atom,
    R: Atom,
    H: Handler<P, NP, R>,
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
            client.sink.send(m).unwrap();
        }
        Message::Response { id, error, results } => {
            if let Some(in_flight) = {
                let mut queue = client.queue.lock()?;
                queue.in_flight_requests.remove(&id)
            } {
                in_flight
                    .tx
                    .send(Message::Response { id, error, results })
                    .unwrap();
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
    fn new(_protocol: Protocol<P, NP, R>) -> Self {
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
