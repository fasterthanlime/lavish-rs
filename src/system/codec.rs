use super::super::facts::{self, Factual, Mapping};
use super::super::{Atom, Message};
use super::Error;

use std::io::{self, Cursor, Read, Write};
use std::marker::PhantomData;

const MAX_MESSAGE_SIZE: usize = 128 * 1024;

pub struct Encoder<M, P, NP, R, IO>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
    IO: Write,
{
    phantom: PhantomData<(P, NP, R)>,
    buffer: Vec<u8>,
    mapping: M,
    write: IO,
}

impl<M, P, NP, R, IO> Encoder<M, P, NP, R, IO>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
    IO: Write,
{
    pub fn new(write: IO, mapping: M) -> Self {
        let buffer: Vec<u8> = vec![0; MAX_MESSAGE_SIZE];
        Self {
            write,
            buffer,
            mapping,
            phantom: PhantomData,
        }
    }

    pub fn encode(&mut self, item: Message<M, P, NP, R>) -> Result<(), Error> {
        let payload_slice = {
            let mut cursor = Cursor::new(&mut self.buffer[..]);
            item.write(&self.mapping, &mut cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let written = cursor.position() as usize;
            &self.buffer[..written]
        };

        let mut length_buffer = vec![0; 16];
        let length_slice = {
            let mut cursor = Cursor::new(&mut length_buffer);
            (payload_slice.len() as u64)
                .write(&self.mapping, &mut cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            let written = cursor.position() as usize;
            &length_buffer[..written]
        };

        self.write.write_all(length_slice)?;
        self.write.write_all(payload_slice)?;
        Ok(())
    }
}

pub struct Decoder<M, P, NP, R, IO>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
    IO: Read,
{
    reader: facts::Reader<IO>,
    phantom: PhantomData<(M, P, NP, R)>,
}

impl<M, P, NP, R, IO> Decoder<M, P, NP, R, IO>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
    IO: Read,
{
    pub fn new(read: IO) -> Self {
        Self {
            reader: facts::Reader::new(read),
            phantom: PhantomData,
        }
    }

    pub fn decode(&mut self) -> Result<Option<Message<M, P, NP, R>>, Error> {
        match self.reader.read_int::<u64>() {
            Ok(_) => { /* ignore length */ }
            Err(e) => {
                match &e {
                    facts::Error::MarkerReadError(rmp::decode::MarkerReadError(e)) => {
                        match e.kind() {
                            std::io::ErrorKind::UnexpectedEof => return Ok(None),
                            _ => {}
                        }
                    }
                    _ => {}
                };
                return Err(io::Error::new(io::ErrorKind::Other, e).into());
            }
        };

        let payload = Message::<M, P, NP, R>::read(&mut self.reader)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(Some(payload))
    }
}
