use std::io::Write;

use byteorder::WriteBytesExt;
use integer_encoding::VarIntWriter;

use super::{Topic, Message, SDO, Field, Data, DataType, fields::{PAGE_SIZE, TIMEOUT}};

#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum Error {
    #[error("io error")]
    Io(#[from] std::io::Error),
}

pub fn encode_field(header: &Field, data: &Option<Data>) -> Result<Vec<u8>, Error> {
    let mut buf = vec![];
    let has_null = false;
    buf.write_u8(((header.data_type as u8) << 3) + ((header.wire_type as u8) << 1) + u8::from(has_null))?;

    buf.write_varint((header.field_id.unwrap_or(0) << 1) + u32::from(header.data_type == DataType::DateTime))?;

    match &data {
        Some(Data::StringW(array)) => {
            for str in array.iter().flatten() {
                buf.write_varint(str.len() + 1)?;
                buf.write_u8(0)?;
                buf.write_all(str.as_bytes())?;
            }
        },
        Some(Data::AsciiString(array)) => {
            for str in array.iter().flatten() {
                buf.write_varint(str.0.len() + 1)?;
                buf.write_all(str.0.as_bytes())?;
            }
        },
        Some(Data::Long(array) | Data::Short(array)) => {
            for value in array.iter().flatten() {
                buf.write_varint(**value)?;
            }
        },
        Some(Data::SDO(array)) => {
            for sdo in array.iter().flatten() {
                buf.write_all(&sdo.encode()?)?;
            }
        },
        Some(Data::Bool(array)) => {
            // buf.write_u8(0x28);
            let mut bytes = vec![];
            let mut n = 0;
            let mut s = 0u8;
            for (_r, value) in array.iter().enumerate() {
                if let Some(value) = value {
                    if n > 0 && n % 8 == 0 {
                        bytes.push(s);
                        s = 0;
                        n = 0;
                    }
                    s += u8::from(*value) << (7 - (n % 8));
                    n += 1;
                }
            }
            if n > 0 {
                bytes.push(s);
            }
            // buf.write_varint(bytes.len());
            for b in bytes {
                buf.write_u8(b)?;
            }
        }
        type_ => warn!(?type_, "unknown type")
    }
    Ok(buf)
}

impl SDO {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];
        buf.write_u8(0x17)?;
        buf.write_varint((self.topic as i32 + 1) as u32)?;
        for (header, data) in &self.fields {
            buf.write_all(&encode_field(header, data)?)?;
        }
        buf.write_u8(0)?;
        Ok(buf)
    }
}

impl Message {
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let mut buf = vec![];
        let mut header_sdo = SDO::new(Topic::UndefinedTopic);
        header_sdo.push_string_w(74, vec![self.id.clone()]);
        buf.write_all(&header_sdo.encode()?)?;

        let mut payload = self.sdo.clone();
        if self.timeout.is_some() {
            payload.push_string_w(TIMEOUT, self.timeout.clone());
        }
        payload.push_string_w(PAGE_SIZE, Some(self.page_size.unwrap_or(1000).to_string()));
        buf.write_all(&payload.encode()?)?;

        Ok(buf)
    }
}

