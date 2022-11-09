use std::{io::{Cursor, Read}, mem::transmute};

use byteorder::{ReadBytesExt, BigEndian, LittleEndian};
use integer_encoding::{VarIntReader, VarIntWriter};
use time::{OffsetDateTime, macros::datetime};

use super::{AsciiString, WireType, DataType, Topic, Data, SDO, Message, Field};

static REF_DATETIME: OffsetDateTime = datetime!(2014-01-01 0:00 UTC);

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
    #[error("invalid length of null data")]
    InvalidLengthOfNullData,
    #[error("invalid length of extra info")]
    InvalidLengthOfExtraInfo,
    #[error("invalid header id")]
    InvalidHeaderId,
    #[error("missing datetime precision")]
    MissingDateTimePrecision,
    #[error("invalid datetime precision")]
    InvalidDateTimePrecision,
}

fn decode_field(buf: &mut Cursor<Vec<u8>>, single_row: bool) -> Result<Option<(Field, Option<Data>)>, Error> {
    let mut field = Field::new(single_row);
    let headers = decode_field_header(buf, &mut field)?;
    if headers { 
        let values = decode_field_values(buf, &field)?;
        Ok(Some((field, Some(values))))
    } else {
        Ok(None)
    }
}

/// var r = 0,
///         i = false,
///         n = 0,
///         o = false,
///         s = 0,
///         f = 0;
///     if (0 === (r = e.readUint8())) return false;
///     if (((t.dataType = r >> 3), (t.wireType = (r >> 1) & 3), (i = 1 == (1 & r)), (r = e.readVarint32()), (t.fieldId = r >> 1), (o = 1 == (1 & r)), t.singleRow)) i && (t.nullFlags = [128]);
///     else if (((t.rows = e.readVarint32()), i && (n = Math.ceil(t.rows / 8)) > 0)) {
///         if (e.limit - e.offset < n) throw "Invalid length of null data.";
///         for (f = e.offset + n, t.nullFlags = []; e.offset < f; ) t.nullFlags.push(e.readUint8());
///     }
///     if (o && (s = e.readVarint32()) > 0) {
///         if (e.limit - e.offset < s) throw "Invalid length of extra info.";
///         for (f = e.offset + s, t.extraInfo = []; e.offset < f; ) t.extraInfo.push(e.readUint8());
///     }
///     return true;
#[instrument(skip(buf, field))]
fn decode_field_header(buf: &mut Cursor<Vec<u8>>, field: &mut Field) -> Result<bool, Error> {
    let r1 = buf.read_u8()?;
    if r1 == 0 {
        return Ok(false);
    }
    field.data_type = (r1 >> 3).into();
    field.wire_type = ((r1 >> 1) & 3).into();
    let has_null = 1 == (1 & r1);
    trace!("r1 = {r1:x}, {dt:?}, {wt:?}, has_null = {has_null}", dt = field.data_type, wt = field.wire_type);

    // 9a01 == 77
    let r2: u32 = buf.read_varint()?;
    field.field_id = Some(r2 >> 1);
    let o = 1 == (1 & r2);
    let mut r22 = vec![];
    r22.write_varint(r2)?;
    trace!("r2 = {r22:x?}; o = {o:?}; field_id = {id:?}", id = field.field_id);

    if field.single_row {
        if has_null {
            field.null_flags = Some(vec![128]);
        }
    } else {
        field.rows = buf.read_varint()?;
        trace!("rows = {rows:x}", rows = field.rows);
        if has_null {
            let n = field.rows.div_ceil(8) as usize;
            if n > 0 {
                // if (buf.get_ref().len() - buf.position() as usize) < n {
                //     return Err(Error::InvalidLengthOfNullData);
                // }
                field.null_flags = Some(vec![]);
                for _ in 0..n {
                    field.null_flags.as_mut().unwrap().push(buf.read_u8()?);
                }
            }
        }
    }
    if o {
        let s: u32 = buf.read_varint()?;
        trace!("reading extra s = {s:x}");
        if s > 0 {
            // if (buf.get_ref().len() - buf.position() as usize) < s {
            //     return Err(Error::InvalidLengthOfExtraInfo);
            // }
            field.extra_info = Some(vec![]);
            for _ in 0..s {
                field.extra_info.as_mut().unwrap().push(buf.read_u8()?);
            }
        }
    }

    Ok(true)
}

#[instrument(skip(buf, field))]
fn decode_field_values(buf: &mut Cursor<Vec<u8>>, field: &Field) -> Result<Data, Error> {
    match field.data_type {
        DataType::StringW => Ok(Data::StringW(read_rows::<String>(buf, field)?)),
        DataType::EncString | DataType::String => Ok(Data::AsciiString(read_rows::<AsciiString>(buf, field)?)),
        DataType::Boolean => Ok(Data::Bool(read_bool_rows(buf, field)?)),
        DataType::Long => Ok(Data::Long(read_rows::<u32>(buf, field)?)),
        DataType::LongLong => Ok(Data::LongLong(read_rows::<u64>(buf, field)?)),
        DataType::Short => Ok(Data::Short(read_rows::<u32>(buf, field)?)),
        DataType::SDO => Ok(Data::SDO(read_rows::<SDO>(buf, field)?)),
        DataType::Double => Ok(Data::Double(read_rows::<f64>(buf, field)?)),
        DataType::Float => Ok(Data::Float(read_rows::<f32>(buf, field)?)),
        DataType::DateTime => Ok(Data::DateTime(read_rows::<OffsetDateTime>(buf, field)?)),
        DataType::Char => Ok(Data::Char(read_rows::<char>(buf, field)?)),
        DataType::Binary => Ok(Data::Binary(read_rows::<Vec<u8>>(buf, field)?)),
        _ => {
            error!(data_type = ?field.data_type, "unknown data type");
            Ok(Data::Unknown)
        },
    }
}

impl ReadType for char {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<Self>, Error> {
        Ok(Box::new(char::from_u32(u32::from(buf.read_u8()?)).unwrap_or_default()))
    }
}

impl ReadType for AsciiString {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<Self>, Error> {
        let len: u32 = buf.read_varint()?;
        if len > 0 {
            let mut str = vec![0; len as usize];
            buf.read_exact(&mut str)?;
            return Ok(Box::new(AsciiString(String::from_utf8_lossy(&str).to_string())))
        }
        Ok(Box::default())
    }
}

impl ReadType for Vec<u8> {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<Vec<u8>>, Error> {
        let len: u32 = buf.read_varint()?;
        if len > 0 {
            let mut bytes = vec![0; len as usize];
            buf.read_exact(&mut bytes)?;
            return Ok(Box::new(bytes))
        }
        Ok(Box::default())
    }
}

impl ReadType for OffsetDateTime {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, header: &Field) -> Result<Box<Self>, Error> {
        let Some(ref extra_info) = header.extra_info else {
            return Err(Error::MissingDateTimePrecision)
        };
        if extra_info.len() != 1 {
            return Err(Error::MissingDateTimePrecision)
        }
        let ms: u64 = buf.read_varint()?;
        match extra_info.first() {
            Some(3) => { // o.DateTimePrecision.Seconds:
                Ok(Box::new(REF_DATETIME + time::Duration::seconds(ms as i64)))
            },
            Some(2) => { // o.DateTimePrecision.Milliseconds:
                Ok(Box::new(REF_DATETIME + time::Duration::milliseconds(ms as i64)))
            },
            Some(1) => { // o.DateTimePrecision.Microseconds:
                if ms >= i64::MAX as u64 - 1 {
                    warn!(?ms, ?extra_info, "received invalid datetime");
                    return Ok(Box::new(REF_DATETIME));
                }
                Ok(Box::new(REF_DATETIME + time::Duration::microseconds(ms as i64))) // FIXME
            },
            Some(0) => { // o.DateTimePrecision.Nanoseconds:
                Ok(Box::new(REF_DATETIME + time::Duration::microseconds((ms / 1000) as i64)))
            },
            _ => Err(Error::InvalidDateTimePrecision)
        }
    }
}

impl ReadType for String {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<Self>, Error> {
        // expect 4221
        let mut len: u32 = buf.read_varint()?;
        if len > 0 {
            len -= 1;
            match buf.read_u8()? {
                0 => {
                    // trace!(?len, remaining = buf.remaining_slice().len(), "decoding utf8 string");
                    let mut str = vec![0; len as usize];
                    buf.read_exact(&mut str)?;
                    return Ok(Box::new(String::from_utf8(str).unwrap()))
                },
                1 => {
                    // trace!(?len, "decoding utf16 string");
                    let mut str = vec![];
                    for _ in 0..len {
                        str.push(buf.read_u16::<BigEndian>().unwrap());
                    }
                    return Ok(Box::new(String::from_utf16(&str).unwrap()))
                },
                o => {
                    warn!("The unicode encoding type '{o}' is invalid and could not be decoded.");
                }
            }
        }
        Ok(Box::default())
    }
}

impl ReadType for u32 {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<u32>, Error> {
        let l = buf.read_varint()?;
        Ok(Box::new(l))
    }
}

impl ReadType for u64 {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<u64>, Error> {
        let l = buf.read_varint()?;
        Ok(Box::new(l))
    }
}

impl ReadType for f64 {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, header: &Field) -> Result<Box<f64>, Error> {
        let double = if header.wire_type == WireType::Varint {
            let l: u32 = buf.read_varint()?;
            f64::from(l)
        } else { // Bit64
            buf.read_f64::<LittleEndian>()?
        };
        Ok(Box::new(double))
    }
}

impl ReadType for f32 {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, header: &Field) -> Result<Box<f32>, Error> {
        let double = if header.wire_type == WireType::Varint {
            let l: u32 = buf.read_varint()?;
            l as f32
        } else { // Bit64
            buf.read_f32::<BigEndian>()?
        };

        // t.wireType === o.WireType.Varint ? o.readArray(e, t, o.readDoubleAsVarint64) : o.readArray(e, t, o.readDouble);
        Ok(Box::new(double))
    }
}

impl ReadType for SDO {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, _header: &Field) -> Result<Box<SDO>, Error> {
        let sdo = read_sdo(buf)?;
        Ok(Box::new(sdo))
    }
}

pub(crate) trait ReadType {
    fn read_bytes(buf: &mut Cursor<Vec<u8>>, header: &Field) -> Result<Box<Self>, Error>;
}

// e, r
fn read_rows<T: ReadType>(buf: &mut Cursor<Vec<u8>>, field: &Field) -> Result<Vec<Option<Box<T>>>, Error> {
    let mut values = vec![];
    for i in 0..field.rows {
        if let Some(ref null_flags) = field.null_flags && null_flags[i.div_floor(8) as usize] & (1 << (7 - (i % 8))) != 0 {
            values.push(None);
        } else {
            values.push(Some(T::read_bytes(buf, field)?));
            
        }
    }
    Ok(values)
}

fn read_bool_rows(buf: &mut Cursor<Vec<u8>>, field: &Field) -> Result<Vec<Option<bool>>, Error> {
    let mut values = vec![];
    let mut r = 0;
    let mut n = None;
    for i in 0..field.rows {
        if let Some(ref null_flags) = field.null_flags && null_flags[i.div_floor(8) as usize] & (1 << (7 - (i % 8))) != 0 {
            values.push(None);
        } else {
            if !(n.is_some() && 8 != r) {
                n = Some(buf.read_u8()?);
                r = 0;
            }
            if let Some(n) = n {
                values.push(Some(1 == ((n >> (7 - r)) & 1)));
            }
            r += 1;
        }
    }
    Ok(values)
}

#[instrument(skip(buf))]
pub fn read_sdo(buf: &mut Cursor<Vec<u8>>) -> Result<SDO, Error> {
    let (single_row, version, o) = {
        let i = buf.read_u8()?;
        let version = i & 0x0f;
        let n = 16 == 16 & i;
        let o = i >> 5;
        (n, version, o)
    };
    if o > 0 {
        buf.set_position(buf.position() + u64::from(o));
    }
    let topic = buf.read_varint::<u32>()? as i32 - 1;
    let topic: Topic = unsafe { transmute(topic) };
    trace!(?single_row, ?version, ?topic, ?o);
    let mut fields = vec![];
    while buf.position() < buf.get_ref().len() as u64 {
        // decode field
        if let Some(field) = decode_field(buf, single_row)? {
            trace!(?field);
            fields.push(field);
        } else {
            break;
        }
    }
    Ok(SDO {
        topic,
        fields
    })
}

#[instrument(skip(buf))]
pub fn read_msg(buf: &mut Cursor<Vec<u8>>) -> Result<Message, Error> {
    trace!("decoding message header");
    let header = read_sdo(buf)?;
    trace!(?header);
    let id = if header.topic == Topic::UndefinedTopic {
        if let Some(field) = header.fields.get(0) {
            match field.1.as_ref().unwrap() {
                Data::StringW(ref msg_id) => Some(msg_id.get(0).and_then(Option::as_ref).unwrap().to_string()),
                Data::AsciiString(ref msg_id) => msg_id.get(0).and_then(Option::as_ref).map(|s| s.0.to_string()),
                Data::Short(ref msg_id) => Some(msg_id.get(0).and_then(Option::as_ref).unwrap().to_string()),
                _ => {
                    warn!(data_type = ?field.0.data_type);
                    return Err(Error::InvalidHeaderId);
                }
            }
        } else {
            None
        }
    } else {
        None
    };
    trace!(?id, "decoding message body");
    Ok(Message {
        id,
        sdo: read_sdo(buf)?,
        page_size: None,
        timeout: None,
    })
}


