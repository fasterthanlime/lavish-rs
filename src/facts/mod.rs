use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::hash::Hash;
use std::marker::Sized;

use byteorder::{self, ReadBytesExt, WriteBytesExt};

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
    WrongExtForTimestamp(i8),
    WrongLenForTimestamp(u8),
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

/**********************************************************************
 * Main trait
 **********************************************************************/

pub trait Mapping: Default + Debug + Send + Sync + 'static {}

pub trait Factual<M>
where
    M: Mapping,
{
    fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), Error>;

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error>
    where
        Self: Sized;

    #[inline]
    fn subread<R: Read, T>(rd: &mut Reader<R>) -> Result<T, Error>
    where
        Self: Sized,
        T: Factual<M>,
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
    pub fn read_bin_len(&mut self) -> Result<usize, Error> {
        let marker = self.fetch_marker()?;
        Ok(match marker {
            Marker::Bin8 => rmp::decode::read_data_u8(self)? as usize,
            Marker::Bin16 => rmp::decode::read_data_u16(self)? as usize,
            Marker::Bin32 => rmp::decode::read_data_u32(self)? as usize,
            _ => return Err(ValueReadError::TypeMismatch(marker).into()),
        })
    }

    #[inline]
    pub fn read_bin(&mut self) -> Result<Vec<u8>, Error> {
        let len = self.read_bin_len()?;
        let mut res = vec![0u8; len];
        self.read_exact(&mut res)?;
        Ok(res)
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

impl<T, M> Factual<M> for Option<T>
where
    T: Factual<M>,
    M: Mapping,
{
    fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), Error> {
        match self {
            Some(v) => v.write(mapping, wr)?,
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

impl<'a, M> Factual<M> for &'a str
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_str(wr, self)?;
        Ok(())
    }

    fn read<R: Read>(_rd: &mut Reader<R>) -> Result<Self, Error> {
        unimplemented!()
    }
}

impl<M> Factual<M> for String
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
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

impl<M> Factual<M> for chrono::DateTime<chrono::offset::Utc>
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        // see hmappingps://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type

        let tv_sec: i64 = self.timestamp();
        let tv_nsec: u32 = self.timestamp_subsec_nanos();

        if tv_sec >> 34 == 0 {
            #[allow(clippy::cast_lossless)]
            let data64: u64 = ((tv_nsec as u64) << 34) | tv_sec as u64;
            if data64 & 0xFFFF_FFFF_0000_0000 == 0 {
                // timestamp 32
                let data32 = data64 as u32;
                wr.write_u8(Marker::FixExt4.to_u8())?;
                wr.write_i8(-1)?;
                wr.write_u32::<byteorder::BigEndian>(data32)?;
            } else {
                // timestamp 64
                wr.write_u8(Marker::FixExt8.to_u8())?;
                wr.write_i8(-1)?;
                wr.write_u64::<byteorder::BigEndian>(data64)?;
            }
        } else {
            // timestamp 96
            // note: Ext8 is followed by 1 byte that specifies the data length
            // and our data length is 4 + 8 = 12. (nanos as a 32-bit uint,
            // seconds as a 64-bit signed int)
            wr.write_u8(Marker::Ext8.to_u8())?;
            wr.write_u8(12)?;
            wr.write_i8(-1)?;
            wr.write_u32::<byteorder::BigEndian>(tv_nsec)?;
            wr.write_i64::<byteorder::BigEndian>(tv_sec)?;
        }

        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        // see hmappingps://github.com/msgpack/msgpack/blob/master/spec.md#timestamp-extension-type

        let marker = rd.fetch_marker()?;
        match marker {
            Marker::FixExt4 => {
                let typ = rd.read_i8()?;
                if typ != -1 {
                    return Err(Error::WrongExtForTimestamp(typ));
                }

                let data32 = rd.read_u32::<byteorder::BigEndian>()?;
                #[allow(clippy::cast_lossless)]
                let naive = chrono::NaiveDateTime::from_timestamp(data32 as i64, 0);
                Ok(chrono::DateTime::<chrono::offset::Utc>::from_utc(
                    naive,
                    chrono::offset::Utc,
                ))
            }
            Marker::FixExt8 => {
                let typ = rd.read_i8()?;
                if typ != -1 {
                    return Err(Error::WrongExtForTimestamp(typ));
                }

                let data64 = rd.read_u64::<byteorder::BigEndian>()?;
                let tv_nsec = data64 >> 34;
                let tv_sec = data64 & 0x0000_0003_FFFF_FFFF;

                #[allow(clippy::cast_lossless)]
                let naive = chrono::NaiveDateTime::from_timestamp(tv_sec as i64, tv_nsec as u32);
                Ok(chrono::DateTime::<chrono::offset::Utc>::from_utc(
                    naive,
                    chrono::offset::Utc,
                ))
            }
            Marker::Ext8 => {
                let len = rd.read_u8()?;
                if len != 12 {
                    return Err(Error::WrongLenForTimestamp(len));
                }

                let typ = rd.read_i8()?;
                if typ != -1 {
                    return Err(Error::WrongExtForTimestamp(typ));
                }

                let tv_nsec = rd.read_u32::<byteorder::BigEndian>()?;
                let tv_sec = rd.read_i64::<byteorder::BigEndian>()?;
                let naive = chrono::NaiveDateTime::from_timestamp(tv_sec, tv_nsec);
                Ok(chrono::DateTime::<chrono::offset::Utc>::from_utc(
                    naive,
                    chrono::offset::Utc,
                ))
            }
            _ => Err(ValueReadError::TypeMismatch(marker).into()),
        }
    }
}

impl<'a, T, M> Factual<M> for &'a [T]
where
    T: Factual<M>,
    M: Mapping,
{
    fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_array_len(wr, self.len() as u32)?;
        for item in *self {
            item.write(mapping, wr)?;
        }

        Ok(())
    }

    fn read<R: Read>(_rd: &mut Reader<R>) -> Result<Self, Error> {
        unimplemented!()
    }
}

impl<T, M> Factual<M> for Vec<T>
where
    T: Factual<M>,
    M: Mapping,
{
    fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_array_len(wr, self.len() as u32)?;
        for item in self {
            item.write(mapping, wr)?;
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
impl<K, V, M> Factual<M> for HashMap<K, V>
where
    K: Factual<M> + Hash + Eq,
    V: Factual<M>,
    M: Mapping,
{
    fn write<W: Write>(&self, mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_map_len(wr, self.len() as u32)?;
        for (k, v) in self {
            k.write(mapping, wr)?;
            v.write(mapping, wr)?;
        }
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        let len = rd.read_map_len()?;

        let mut res = Self::new();
        for _ in 0..len {
            res.insert(Self::subread(rd)?, Self::subread(rd)?);
        }
        Ok(res)
    }
}

#[derive(Debug, PartialEq)]
pub struct Bin(pub Vec<u8>);

impl From<Vec<u8>> for Bin {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl AsRef<[u8]> for Bin {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl<M> Factual<M> for Bin
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_bin(wr, &self.0)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(Self(rd.read_bin()?))
    }
}

impl<M> Factual<M> for i8
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_sint(wr, *self as i64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for i16
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_sint(wr, *self as i64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for i32
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_sint(wr, *self as i64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for i64
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_sint(wr, *self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for u8
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_uint(wr, *self as u64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for u16
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_uint(wr, *self as u64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for u32
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        #[allow(clippy::cast_lossless)]
        rmp::encode::write_uint(wr, *self as u64)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for u64
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_uint(wr, *self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_int()?)
    }
}

impl<M> Factual<M> for bool
where
    M: Mapping,
{
    fn write<W: Write>(&self, _mapping: &M, wr: &mut W) -> Result<(), Error> {
        rmp::encode::write_bool(wr, *self)?;
        Ok(())
    }

    fn read<R: Read>(rd: &mut Reader<R>) -> Result<Self, Error> {
        Ok(rd.read_bool()?)
    }
}

pub fn write<M, T, W>(t: &T, mapping: &M, wr: &mut W) -> Result<(), Error>
where
    T: Factual<M>,
    W: Write,
    M: Mapping,
{
    t.write(mapping, wr)
}

pub fn read<M, T, R>(rd: &mut Reader<R>) -> Result<T, Error>
where
    T: Factual<M>,
    R: Read,
    M: Mapping,
{
    T::read(rd)
}

impl Mapping for () {}

pub fn read_simple<T, R>(rd: &mut Reader<R>) -> Result<T, Error>
where
    T: Factual<()>,
    R: Read,
{
    T::read(rd)
}

pub struct OffsetList(pub Vec<i32>);

impl OffsetList {
    #[inline]
    pub fn get(&self, index: usize) -> Option<u32> {
        let i = self.0[index];
        if i < 0 {
            None
        } else {
            Some(i as u32)
        }
    }
}

pub enum TranslationTable {
    Mapped(OffsetList),
    Incompatible(String),
}

impl TranslationTable {
    pub fn validate(&self) -> Result<&OffsetList, Error> {
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

#[cfg(test)]
mod tests {
    use super::{Factual, Mapping, Reader};
    use netbuf::Buf;
    use std::cmp::PartialEq;
    use std::collections::HashMap;
    use std::fmt::Debug;

    #[test]
    fn numbers() -> Result<(), Box<std::error::Error>> {
        cycle_simple(2i8.pow(6))?;
        cycle_simple(2i16.pow(14))?;
        cycle_simple(2i32.pow(30))?;
        cycle_simple(2i64.pow(62))?;

        cycle_simple(2u8.pow(7))?;
        cycle_simple(2u16.pow(15))?;
        cycle_simple(2u32.pow(31))?;
        cycle_simple(2u64.pow(63))?;

        Ok(())
    }

    #[test]
    fn bools() -> Result<(), Box<std::error::Error>> {
        cycle_simple(true)?;
        cycle_simple(false)?;

        Ok(())
    }

    #[test]
    fn strings() -> Result<(), Box<std::error::Error>> {
        cycle_simple("".to_string())?;
        cycle_simple("dull".repeat(128).to_string())?;
        cycle_simple("dull".repeat(1024).to_string())?;

        Ok(())
    }

    #[test]
    fn timestamps() -> Result<(), Box<std::error::Error>> {
        fn cycle_timestamp(
            secs: i64,
            nsecs: u32,
            marker_expected: rmp::Marker,
        ) -> Result<(), Box<std::error::Error>> {
            let v = chrono::NaiveDateTime::from_timestamp(secs, nsecs);
            let v = chrono::DateTime::<chrono::offset::Utc>::from_utc(v, chrono::offset::Utc);
            cycle_simple(v.clone())?;

            let mut buf = Buf::new();
            v.write(&(), &mut buf)?;
            let mut slice = &buf[..];
            let marker_actual = rmp::decode::read_marker(&mut slice).unwrap();
            assert_eq!(marker_expected, marker_actual);

            Ok(())
        }

        cycle_simple(chrono::offset::Utc::now())?;

        // epoch
        cycle_timestamp(0, 0, rmp::Marker::FixExt4)?;
        // time at the writing of this test
        cycle_timestamp(1561378047, 0, rmp::Marker::FixExt4)?;

        // time at the writing of this test, with some nanoseconds
        cycle_timestamp(1561378047, 2398, rmp::Marker::FixExt8)?;

        // some time in the future (year 2200), no nanoseconds
        cycle_timestamp(7273195896, 0, rmp::Marker::FixExt8)?;

        // some time in the future (year 2200), with nanoseconds
        cycle_timestamp(7273195896, 23549, rmp::Marker::FixExt8)?;

        // time of moon landing (before epoch)
        cycle_timestamp(-14182980, 0, rmp::Marker::Ext8)?;

        // some time far into the future (year 2600 - amos's 610th birthday)
        cycle_timestamp(19898323200, 0, rmp::Marker::Ext8)?;

        // some time far into the future, with nanos
        cycle_timestamp(19898323200, 2359807, rmp::Marker::Ext8)?;

        Ok(())
    }

    #[test]
    fn arrays() -> Result<(), Box<std::error::Error>> {
        let strings = vec!["foo".to_string(), "bar".to_string(), "dull".repeat(128)];
        cycle_simple(strings)?;

        Ok(())
    }

    #[test]
    fn maps() -> Result<(), Box<std::error::Error>> {
        let mut map = HashMap::<String, i32>::new();
        cycle_simple(map.clone())?;

        map.insert("ten".into(), 10);
        map.insert("twenty eight".into(), 28);
        map.insert("one hundred and thirty nine".into(), 139);
        cycle_simple(map.clone())?;

        Ok(())
    }

    #[test]
    fn bins() -> Result<(), Box<std::error::Error>> {
        let bin: Vec<u8> = vec![];
        cycle_simple(super::Bin(bin))?;

        let bin: Vec<u8> = vec![6, 123, 92, 32, 41, 0, 14, 28, 255];
        cycle_simple(super::Bin(bin))?;

        let mut bin: Vec<u8> = Vec::new();
        for _ in 0..1024 {
            bin.push(6);
            bin.push(92);
            bin.push(0);
            bin.push(32);
        }
        cycle_simple(super::Bin(bin))?;

        Ok(())
    }

    fn cycle_simple<T>(l: T) -> Result<(), Box<std::error::Error>>
    where
        T: Factual<()> + Debug + PartialEq,
    {
        cycle((), l)
    }

    fn cycle<T, M>(mapping: M, l: T) -> Result<(), Box<std::error::Error>>
    where
        T: Factual<M> + Debug + PartialEq,
        M: Mapping,
    {
        let mut buf = Buf::new();
        l.write(&mapping, &mut buf)?;
        let mut slice = &buf[..];
        let r: T = super::read(&mut Reader::new(&mut slice))?;
        assert_eq!(l, r);
        Ok(())
    }
}
