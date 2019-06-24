use std::fmt::Debug;
use std::marker::PhantomData;
use std::marker::Send;

use super::facts::{self, Mapping};

pub trait PendingRequests {
    fn get_pending(&self, id: u32) -> Option<&'static str>;
}

pub trait Atom<M>: facts::Factual<M> + Debug + Sized + Send + 'static
where
    M: Mapping,
{
    fn method(&self) -> &'static str;
}

#[derive(Debug)]
pub enum Message<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    Request {
        id: u32,
        params: P,
        phantom: PhantomData<M>,
    },
    Response {
        id: u32,
        error: Option<String>,
        results: Option<R>,
        phantom: PhantomData<M>,
    },
    Notification {
        params: NP,
        phantom: PhantomData<M>,
    },
}

impl<M, P, NP, R> Message<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    pub fn request(id: u32, params: P) -> Self {
        Message::<M, P, NP, R>::Request {
            id,
            params,
            phantom: PhantomData,
        }
    }

    pub fn notification(params: NP) -> Self {
        Message::<M, P, NP, R>::Notification {
            params,
            phantom: PhantomData,
        }
    }

    pub fn response(id: u32, error: Option<String>, results: Option<R>) -> Self {
        Message::<M, P, NP, R>::Response {
            id,
            error,
            results,
            phantom: PhantomData,
        }
    }
}

use std::io::{Read, Write};

impl<M, P, NP, R> facts::Factual<M> for Message<M, P, NP, R>
where
    P: Atom<M>,
    NP: Atom<M>,
    R: Atom<M>,
    M: Mapping,
{
    fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), facts::Error> {
        match self {
            Message::Request { id, params, .. } => {
                rmp::encode::write_array_len(wr, 3)?;
                0.write(mapping, wr)?;
                id.write(mapping, wr)?;
                params.write(mapping, wr)?;
            }
            Message::Response {
                id, error, results, ..
            } => {
                rmp::encode::write_array_len(wr, 4)?;
                1.write(mapping, wr)?;
                id.write(mapping, wr)?;
                error.write(mapping, wr)?;
                results.write(mapping, wr)?;
            }
            Message::Notification { params, .. } => {
                rmp::encode::write_array_len(wr, 2)?;
                2.write(mapping, wr)?;
                params.write(mapping, wr)?;
            }
        }
        Ok(())
    }

    fn read<Rd: Read>(rd: &mut facts::Reader<Rd>) -> Result<Self, facts::Error>
    where
        Self: Sized,
    {
        let len = rd.read_array_len()?;
        let typ: u32 = rd.read_int()?;
        match typ {
            0 => {
                // Request
                if len != 3 {
                    unreachable!()
                }
                Ok(Message::Request {
                    id: Self::subread(rd)?,
                    params: Self::subread(rd)?,
                    phantom: PhantomData,
                })
            }
            1 => {
                // Response
                if len != 4 {
                    unreachable!()
                }
                Ok(Message::Response {
                    id: Self::subread(rd)?,
                    error: Self::subread(rd)?,
                    results: Self::subread(rd)?,
                    phantom: PhantomData,
                })
            }
            2 => {
                // Notification
                if len != 2 {
                    unreachable!()
                }
                Ok(Message::Notification {
                    params: Self::subread(rd)?,
                    phantom: PhantomData,
                })
            }
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use facts::Factual;

    #[derive(Debug, Default)]
    pub struct M {}

    impl facts::Mapping for M {}

    #[derive(Debug)]
    enum Test {
        Foo(TestFoo),
        Bar(TestBar),
    }

    impl facts::Factual<M> for Test {
        fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), facts::Error> {
            rmp::encode::write_array_len(wr, 2)?;
            match self {
                Test::Foo(v) => {
                    0.write(mapping, wr)?;
                    v.write(mapping, wr)?;
                }
                Test::Bar(v) => {
                    1.write(mapping, wr)?;
                    v.write(mapping, wr)?;
                }
            }
            Ok(())
        }

        fn read<R: Read>(rd: &mut facts::Reader<R>) -> Result<Self, facts::Error>
        where
            Self: Sized,
        {
            let len = rd.read_array_len()?;
            if len != 2 {
                unreachable!()
            }

            let discriminant: u32 = rd.read_int()?;
            Ok(match discriminant {
                0 => Test::Foo(Self::subread(rd)?),
                1 => Test::Bar(Self::subread(rd)?),
                _ => unreachable!(),
            })
        }
    }

    #[derive(Debug)]
    struct TestFoo {
        val: i64,
    }

    impl facts::Factual<M> for TestFoo {
        fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), facts::Error> {
            self.val.write(mapping, wr)
        }

        fn read<R: Read>(rd: &mut facts::Reader<R>) -> Result<Self, facts::Error>
        where
            Self: Sized,
        {
            Ok(Self {
                val: Self::subread(rd)?,
            })
        }
    }

    #[derive(Debug)]
    struct TestBar {
        val: String,
        bs: facts::Bin,
    }

    impl facts::Factual<M> for TestBar {
        fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), facts::Error> {
            self.val.write(mapping, wr)?;
            self.bs.write(mapping, wr)?;
            Ok(())
        }

        fn read<R: Read>(rd: &mut facts::Reader<R>) -> Result<Self, facts::Error>
        where
            Self: Sized,
        {
            Ok(Self {
                val: Self::subread(rd)?,
                bs: Self::subread(rd)?,
            })
        }
    }

    type Message = super::Message<M, Test, Test, Test>;

    impl Atom<M> for Test {
        fn method(&self) -> &'static str {
            match self {
                Test::Foo(_) => "Foo",
                Test::Bar(_) => "Bar",
            }
        }
    }

    #[test]
    fn internal() {
        cycle(Message::request(420, Test::Foo(TestFoo { val: 69 })));
        cycle(Message::request(
            420,
            Test::Bar(TestBar {
                val: "success!".into(),
                bs: vec![0x0, 0x15, 0x93].into(),
            }),
        ));
    }

    fn cycle(m1: Message) {
        let mapping = M {};
        println!("m1 = {:#?}", m1);

        let mut buf1: Vec<u8> = Vec::new();
        m1.write(&mapping, &mut buf1).unwrap();

        let m2: Message = Message::read(&mut facts::Reader::new(&mut &buf1[..])).unwrap();
        println!("m2 = {:#?}", m2);

        let mut buf2: Vec<u8> = Vec::new();
        m2.write(&mapping, &mut buf2).unwrap();

        assert_eq!(buf1, buf2);
    }
}
