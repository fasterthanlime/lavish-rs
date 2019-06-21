use std::collections::HashMap;
use std::hash::Hash;
use std::marker::Sized;

use std::io::{Read, Write};

use rmp::decode::{DecodeStringError, MarkerReadError, NumValueReadError, ValueReadError};
use rmp::encode::ValueWriteError;
use rmp::Marker;

use num_traits::cast::FromPrimitive;

/**********************************************************************
 * Error type
 **********************************************************************/

#[derive(Debug)]
pub enum Error {
    IO(std::io::Error),
    InvalidStructLength { expected: usize, actual: usize },
    IncompatibleSchema(String),
    DecodeStringError(),
    ValueWriteError(ValueWriteError),
    ValueReadError(ValueReadError),
    NumValueReadError(NumValueReadError),
    MarkerReadError(MarkerReadError),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(_err: std::string::FromUtf8Error) -> Self {
        Error::DecodeStringError()
    }
}

impl From<std::str::Utf8Error> for Error {
    fn from(_err: std::str::Utf8Error) -> Self {
        Error::DecodeStringError()
    }
}

impl From<MarkerReadError> for Error {
    fn from(err: MarkerReadError) -> Self {
        Error::MarkerReadError(err)
    }
}

impl From<ValueWriteError> for Error {
    fn from(err: ValueWriteError) -> Self {
        Error::ValueWriteError(err)
    }
}

impl From<ValueReadError> for Error {
    fn from(err: ValueReadError) -> Self {
        Error::ValueReadError(err)
    }
}

impl From<NumValueReadError> for Error {
    fn from(err: NumValueReadError) -> Self {
        Error::NumValueReadError(err)
    }
}

impl<'a> From<DecodeStringError<'a>> for Error {
    fn from(_err: DecodeStringError<'a>) -> Self {
        Error::DecodeStringError()
    }
}

impl std::error::Error for Error {}

use std::fmt;
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/**********************************************************************
 * Main trait
 **********************************************************************/

pub trait Factual<TT> {
    fn write<W: Write>(&self, tt: &TT, wr: &mut W) -> Result<(), Error>;

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error>
    where
        Self: Sized;

    #[inline]
    fn subread<R: Read, T>(rd: &mut Reader<R>) -> Result<T, Error>
    where
        Self: Sized,
        T: Factual<TT>,
    {
        T::read(rd)
    }
}

pub struct Reader<R>
where
    R: Read,
{
    rd: R,
    buf: Vec<u8>,
    marker: Option<Marker>,
}

impl<R> Reader<R>
where
    R: Read,
{
    pub fn new(rd: R) -> Self {
        Self {
            rd,
            buf: Vec::with_capacity(128),
            marker: None,
        }
    }

    #[inline]
    fn fetch_marker(&mut self) -> Result<Marker, Error> {
        match self.marker.take() {
            Some(marker) => Ok(marker),
            None => Ok(rmp::decode::read_marker(&mut self.rd)?),
        }
    }

    #[inline]
    fn read_slice(&mut self, len: usize) -> Result<&[u8], Error> {
        self.buf.resize(len, 0u8);
        self.rd.read_exact(&mut self.buf[..])?;
        Ok(&self.buf[..])
    }

    #[inline]
    pub fn read_array_len(&mut self) -> Result<usize, Error> {
        let marker = self.fetch_marker()?;
        Ok(match marker {
            Marker::FixArray(len) => len as usize,
            Marker::Array16 => rmp::decode::read_data_u16(self)? as usize,
            Marker::Array32 => rmp::decode::read_data_u32(self)? as usize,
            _ => return Err(ValueReadError::TypeMismatch(marker).into()),
        })
    }

    #[inline]
    pub fn read_map_len(&mut self) -> Result<usize, Error> {
        let marker = self.fetch_marker()?;
        Ok(match marker {
            Marker::FixMap(len) => len as usize,
            Marker::Map16 => rmp::decode::read_data_u16(self)? as usize,
            Marker::Map32 => rmp::decode::read_data_u32(self)? as usize,
            _ => return Err(ValueReadError::TypeMismatch(marker).into()),
        })
    }

    #[inline]
    pub fn read_int<T>(&mut self) -> Result<T, Error>
    where
        T: FromPrimitive,
    {
        let marker = self.fetch_marker()?;
        match marker {
            Marker::FixPos(val) => T::from_u8(val),
            Marker::FixNeg(val) => T::from_i8(val),
            Marker::U8 => T::from_u8(rmp::decode::read_data_u8(self)?),
            Marker::U16 => T::from_u16(rmp::decode::read_data_u16(self)?),
            Marker::U32 => T::from_u32(rmp::decode::read_data_u32(self)?),
            Marker::U64 => T::from_u64(rmp::decode::read_data_u64(self)?),
            Marker::I8 => T::from_i8(rmp::decode::read_data_i8(self)?),
            Marker::I16 => T::from_i16(rmp::decode::read_data_i16(self)?),
            Marker::I32 => T::from_i32(rmp::decode::read_data_i32(self)?),
            Marker::I64 => T::from_i64(rmp::decode::read_data_i64(self)?),
            marker => return Err(NumValueReadError::TypeMismatch(marker).into()),
        }
        .ok_or_else(|| NumValueReadError::OutOfRange.into())
    }

    #[inline]
    pub fn read_bool(&mut self) -> Result<bool, Error> {
        let marker = self.fetch_marker()?;
        match marker {
            Marker::True => Ok(true),
            Marker::False => Ok(false),
            marker => Err(ValueReadError::TypeMismatch(marker).into()),
        }
    }

    #[inline]
    pub fn expect_marker(&mut self, expected: Marker) -> Result<(), Error> {
        let marker = self.fetch_marker()?;
        if marker == expected {
            return Ok(());
        }
        Err(ValueReadError::TypeMismatch(marker).into())
    }

    #[inline]
    pub fn expect_array_len(&mut self, expected: usize) -> Result<(), Error> {
        let actual = self.read_array_len()?;
        if expected != actual {
            return Err(Error::InvalidStructLength { expected, actual });
        }
        Ok(())
    }

    #[inline]
    pub fn read_str_len(&mut self) -> Result<usize, Error> {
        let marker = self.fetch_marker()?;
        Ok(match marker {
            Marker::FixStr(len) => len as usize,
            Marker::Str8 => rmp::decode::read_data_u8(self)? as usize,
            Marker::Str16 => rmp::decode::read_data_u16(self)? as usize,
            Marker::Str32 => rmp::decode::read_data_u32(self)? as usize,
            _ => return Err(ValueReadError::TypeMismatch(marker).into()),
        })
    }
}

impl<R> Read for Reader<R>
where
    R: Read,
{
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.rd.read(buf)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        self.rd.read_exact(buf)
    }
}

impl<T, TT> Factual<TT> for Option<T>
where
    T: Factual<TT>,
{
    fn write<W: Write>(&self, tt: &TT, wr: &mut W) -> Result<(), Error> {
        match self {
            Some(v) => v.write(tt, wr)?,
            None => rmp::encode::write_nil(wr)?,
        };

        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        match rmp::decode::read_marker(rd)? {
            Marker::Null => Ok(None),
            marker => {
                rd.marker = Some(marker);
                Ok(Some(T::read(rd)?))
            }
        }
    }
}

impl<'a, TT> Factual<TT> for &'a str {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_str(wr, self)?;
        Ok(())
    }

    fn read<R: Read>(_rd: &mut Reader<R>) -> Result<Self, Error> {
        unimplemented!()
    }
}

impl<'a, TT> Factual<TT> for String {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_str(wr, self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        let len = rd.read_str_len()?;
        let bytes = rd.read_slice(len)?;
        let res = std::str::from_utf8(bytes)?.to_string();
        Ok(res)
    }
}

impl<'a, T, TT> Factual<TT> for &'a [T]
where
    T: Factual<TT>,
{
    fn write<W: Write>(&self, tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_array_len(wr, self.len() as u32)?;
        for item in *self {
            item.write(tt, wr)?;
        }

        Ok(())
    }

    fn read<R: Read>(_rd: &mut Reader<R>) -> Result<Self, Error> {
        unimplemented!()
    }
}

impl<'a, T, TT> Factual<TT> for Vec<T>
where
    T: Factual<TT>,
{
    fn write<W: Write>(&self, tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_array_len(wr, self.len() as u32)?;
        for item in self {
            item.write(tt, wr)?;
        }

        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        let len = rd.read_array_len()?;

        let mut res = Self::with_capacity(len);
        for _ in 0..len {
            res.push(T::read(rd)?);
        }
        Ok(res)
    }
}

// we'd make clippy happy here, but we also need to call 'new'
// and it's only defined for the default hasher, so, welp.
#[allow(clippy::implicit_hasher)]
impl<'a, K, V, TT> Factual<TT> for HashMap<K, V>
where
    K: Factual<TT> + Hash + Eq,
    V: Factual<TT>,
{
    fn write<W: Write>(&self, tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_map_len(wr, self.len() as u32)?;
        for (k, v) in self {
            k.write(tt, wr)?;
            v.write(tt, wr)?;
        }
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        let len = rd.read_array_len()?;

        let mut res = Self::new();
        for _ in 0..len {
            res.insert(Self::subread(rd)?, Self::subread(rd)?);
        }
        Ok(res)
    }
}

impl<'a, TT> Factual<TT> for i8 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_sint(wr, *self as i64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for i16 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_sint(wr, *self as i64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for i32 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_sint(wr, *self as i64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for i64 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_sint(wr, *self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for u8 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_uint(wr, *self as u64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for u16 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_uint(wr, *self as u64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for u32 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_uint(wr, *self as u64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for u64 {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_uint(wr, *self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<'a, TT> Factual<TT> for bool {
    fn write<W: Write>(&self, _tt: &TT, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_bool(wr, *self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_bool()?)
    }
}

pub fn write<TT, T, W>(t: &T, tt: &TT, wr: &mut W) -> Result<(), Error>
where
    T: Factual<TT>,
    W: Write,
{
    t.write(tt, wr)
}

pub struct OffsetList(pub Vec<i32>);

pub enum TranslationTable {
    Mapped(OffsetList),
    Incompatible(String),
}

impl TranslationTable {
    fn validate(&self) -> Result<&OffsetList, Error> {
        use TranslationTable::*;

        match self {
            Mapped(list) => Ok(&list),
            Incompatible(reason) => Err(Error::IncompatibleSchema(reason.to_owned())),
        }
    }

    pub fn write<F, W>(&self, wr: &mut W, f: F) -> Result<(), Error>
    where
        F: Fn(&mut W, u32) -> Result<(), Error>,
        W: Write,
    {
        let offsets = self.validate()?;
        rmp::encode::write_array_len(wr, offsets.0.len() as u32)?;

        for &i in &offsets.0 {
            if i < 0 {
                rmp::encode::write_nil(wr)?;
            } else {
                f(wr, i as u32)?;
            }
        }

        Ok(())
    }
}

pub struct SchemaInfo {
    pub structs: HashMap<String, StructInfo>,
}

pub struct StructInfo {
    pub fields: Vec<FieldInfo>,
}

pub struct FieldInfo {
    pub name: String,
    pub typ: FieldType,
}

pub enum FieldType {
    Base(BaseType),
    Option(Box<FieldType>),
    List(Box<FieldType>),
    Map(Box<FieldType>, Box<FieldType>),
}

pub enum BaseType {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F32,
    F64,
    Bool,
    String,
    Data,
    Timestamp,
}
