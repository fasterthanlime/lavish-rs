use super::{Atom, Error, Message, PendingRequests};

use std::marker::{PhantomData, Unpin};

use futures::lock::Mutex;
use std::collections::HashMap;
use std::sync::Arc;

use futures::channel::{mpsc, oneshot};
use futures::executor;
use futures::prelude::*;
use futures_codec::Framed;

use futures::task::SpawnExt;

mod codec;
use codec::Codec;

pub trait Conn: AsyncRead + AsyncWrite + Send + Sized + Unpin + 'static {}
impl<T> Conn for T where T: AsyncRead + AsyncWrite + Send + Sized + Unpin + 'static {}

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

pub trait Handler<P, NP, R, FT>: Sync + Send
where
    P: Atom,
    NP: Atom,
    R: Atom,
    FT: Future<Output = Result<R, Error>> + Send + 'static,
{
    fn handle(&self, client: Client<P, NP, R>, params: P) -> FT;
}

impl<P, NP, R, F, FT> Handler<P, NP, R, FT> for F
where
    P: Atom,
    R: Atom,
    NP: Atom,
    F: (Fn(Client<P, NP, R>, P) -> FT) + Send + Sync,
    FT: Future<Output = Result<R, Error>> + Send + 'static,
{
    fn handle(&self, client: Client<P, NP, R>, params: P) -> FT {
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

    #[allow(clippy::needless_lifetimes)]
    pub async fn call_raw(
        &self,
        params: P,
    ) -> Result<Message<P, NP, R>, Box<dyn std::error::Error>> {
        let id = {
            let mut queue = self.queue.lock().await;
            queue.next_id()
        };

        let method = params.method();
        let m = Message::Request { id, params };

        let (tx, rx) = oneshot::channel::<Message<P, NP, R>>();
        let in_flight = InFlightRequest { method, tx };
        {
            let mut queue = self.queue.lock().await;
            queue.in_flight_requests.insert(id, in_flight);
        }

        {
            let mut sink = self.sink.clone();
            sink.send(m).await?;
        }
        Ok(rx.await?)
    }

    #[allow(clippy::needless_lifetimes)]
    pub async fn call<D, RR>(&self, params: P, downgrade: D) -> Result<RR, Error>
    where
        D: Fn(R) -> Option<RR>,
    {
        match self.call_raw(params).await {
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

pub fn connect<C, H, FT, P, NP, R>(
    protocol: Protocol<P, NP, R>,
    handler: H,
    io: C,
    mut pool: executor::ThreadPool,
) -> Result<Client<P, NP, R>, Error>
where
    C: Conn,
    H: Handler<P, NP, R, FT> + 'static,
    FT: Future<Output = Result<R, Error>> + Send + 'static,
    P: Atom,
    NP: Atom,
    R: Atom,
{
    let queue = Arc::new(Mutex::new(Queue::new(protocol)));

    let codec = Codec::new(queue.clone());
    let framed = Framed::new(io, codec);
    let (mut sink, mut stream) = framed.split();
    let (tx, mut rx) = mpsc::channel(128);

    let client = Client::<P, NP, R> {
        queue: queue.clone(),
        sink: tx,
    };

    let ret = client.clone();

    pool.clone().spawn(async move {
        while let Some(m) = rx.next().await {
            sink.send(m).await.unwrap();
        }
    })?;

    pool.clone()
        .spawn(async move {
            let handler = Arc::new(handler);

            while let Some(m) = stream.next().await {
                let res = m.map(|m| pool.spawn(handle_message(m, handler.clone(), client.clone())));
                if let Err(e) = res {
                    eprintln!("message stream error: {:#?}", e);
                }
            }
        })
        .map_err(Error::SpawnError)?;

    Ok(ret)
}

async fn handle_message<P, NP, R, H, FT>(
    inbound: Message<P, NP, R>,
    handler: Arc<H>,
    mut client: Client<P, NP, R>,
) where
    P: Atom,
    NP: Atom,
    R: Atom,
    H: Handler<P, NP, R, FT>,
    FT: Future<Output = Result<R, Error>> + Send + 'static,
{
    match inbound {
        Message::Request { id, params } => {
            let m = match handler.handle(client.clone(), params).await {
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
            client.sink.send(m).await.unwrap();
        }
        Message::Response { id, error, results } => {
            if let Some(in_flight) = {
                let mut queue = client.queue.lock().await;
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
}

struct InFlightRequest<P, NP, R>
where
    P: Atom,
    NP: Atom,
    R: Atom,
{
    method: &'static str,
    tx: oneshot::Sender<Message<P, NP, R>>,
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
