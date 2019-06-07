use super::super::{Atom, Message};
use super::Error;
use super::Queue;

use serde::Serialize;
use std::io::{Cursor, Read, Write};
use std::marker::PhantomData;

use std::sync::{Arc, Mutex};

use log::*;

const MAX_MESSAGE_SIZE: usize = 128 * 1024;

pub struct Encoder<P, NP, R, IO>
where
    P: Atom,
    NP: Atom,
    R: Atom,
    IO: Write,
{
    phantom: PhantomData<(P, NP, R)>,
    buffer: Vec<u8>,
    write: IO,
}

impl<P, NP, R, IO> Encoder<P, NP, R, IO>
where
    P: Atom,
    NP: Atom,
    R: Atom,
    IO: Write,
{
    pub fn new(write: IO) -> Self {
        let buffer: Vec<u8> = vec![0; MAX_MESSAGE_SIZE];
        Self {
            write,
            buffer,
            phantom: PhantomData,
        }
    }

    pub fn encode(&mut self, item: Message<P, NP, R>) -> Result<(), Error> {
        use std::io;

        let payload_slice = {
            let cursor = Cursor::new(&mut self.buffer[..]);
            let mut ser = rmp_serde::Serializer::new_named(cursor);
            item.serialize(&mut ser)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let cursor = ser.into_inner();
            let written = cursor.position() as usize;
            &self.buffer[..written]
        };

        let mut length_buffer = vec![0; 16];
        let length_slice = {
            use serde::ser::Serializer;

            let cursor = Cursor::new(&mut length_buffer);
            let mut ser = rmp_serde::Serializer::new(cursor);
            ser.serialize_u64(payload_slice.len() as u64)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

            let cursor = ser.into_inner();
            let written = cursor.position() as usize;
            &length_buffer[..written]
        };

        self.write.write_all(length_slice)?;
        self.write.write_all(payload_slice)?;
        Ok(())
    }

    pub fn into_inner(self) -> IO {
        self.write
    }
}

pub struct Decoder<P, NP, R, IO>
where
    P: Atom,
    NP: Atom,
    R: Atom,
    IO: Read,
{
    read: IO,
    queue: Arc<Mutex<Queue<P, NP, R>>>,
}

impl<P, NP, R, IO> Decoder<P, NP, R, IO>
where
    P: Atom,
    NP: Atom,
    R: Atom,
    IO: Read,
{
    pub fn new(read: IO, queue: Arc<Mutex<Queue<P, NP, R>>>) -> Self {
        Self { read, queue }
    }

    pub fn decode(&mut self) -> Result<Option<Message<P, NP, R>>, Error> {
        use serde::de::Deserializer;
        use std::error::Error;
        use std::io;

        let mut deser = rmp_serde::Deserializer::new(&mut self.read);
        // deserialize payload len
        match deser.deserialize_u64(LengthVisitor {}) {
            Err(e) => {
                match &e {
                    rmp_serde::decode::Error::InvalidMarkerRead(e) => match e.kind() {
                        std::io::ErrorKind::UnexpectedEof => return Ok(None),
                        _ => {}
                    },
                    _ => {}
                };
                return Err(io::Error::new(io::ErrorKind::Other, e).into());
            }
            Ok(_) => {}
        };

        let pr = self.queue.lock()?;
        let payload = Message::<P, NP, R>::deserialize(&mut deser, &*pr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Some(payload))
    }
}

struct LengthVisitor {}

impl<'de> serde::de::Visitor<'de> for LengthVisitor {
    type Value = u64;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a framed msgpack-rpc payload length")
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(value)
    }
}
