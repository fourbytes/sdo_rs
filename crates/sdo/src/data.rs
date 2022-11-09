use std::fmt::Write;

use itertools::Itertools;
use time::OffsetDateTime;

use crate::SDO;

#[derive(Debug, Clone)]
pub enum Data {
    StringW(Vec<Option<Box<String>>>),
    Bool(Vec<Option<bool>>),
    Long(Vec<Option<Box<u32>>>),
    LongLong(Vec<Option<Box<u64>>>),
    Short(Vec<Option<Box<u32>>>),
    AsciiString(Vec<Option<Box<AsciiString>>>),
    SDO(Vec<Option<Box<SDO>>>),
    Double(Vec<Option<Box<f64>>>),
    Float(Vec<Option<Box<f32>>>),
    DateTime(Vec<Option<Box<OffsetDateTime>>>),
    Char(Vec<Option<Box<char>>>),
    Binary(Vec<Option<Box<Vec<u8>>>>),
    Unknown,
}

impl Data {
    pub fn to_string(&self) -> Option<String> {
        match self {
            Data::StringW(s) => Some(s.iter().filter_map(Option::as_ref).join(", ")),
            Data::AsciiString(s) => {
                Some(s.iter().filter_map(Option::as_ref).map(|o| &o.0).join(", "))
            }
            _ => {
                warn!("tried to parse {self:?} as string");
                None
            }
        }
    }

    pub fn to_vec_sdo(&self) -> Option<Vec<Option<SDO>>> {
        if let Data::SDO(s) = self {
            Some(s.iter().map(|o| o.as_ref().map(|o| *o.clone())).collect())
        } else {
            warn!("tried to parse {self:?} as vec sdo");
            None
        }
    }

    pub fn to_vec_string(&self) -> Option<Vec<Option<String>>> {
        match self {
            Data::StringW(s) => Some(s.iter().map(|o| o.as_deref().cloned()).collect()),
            Data::AsciiString(s) => Some(
                s.iter()
                    .map(|o| o.as_deref().cloned().map(|o| o.0))
                    .collect(),
            ),
            _ => {
                warn!("tried to parse {self:?} as vec string");
                None
            }
        }
    }

    pub fn as_vec_str(&self) -> Option<Vec<Option<&str>>> {
        match self {
            Data::StringW(s) => Some(s.iter().map(|o| o.as_ref().map(|s| s.as_str())).collect()),
            Data::AsciiString(s) => {
                Some(s.iter().map(|o| o.as_ref().map(|s| s.0.as_str())).collect())
            }
            _ => {
                warn!("tried to parse {self:?} as vec string");
                None
            }
        }
    }

    pub fn as_first_str(&self) -> Option<&str> {
        match self {
            Data::StringW(s) => s.iter().find_map(|o| o.as_ref().map(|s| s.as_str())),
            Data::AsciiString(s) => s.iter().find_map(|o| o.as_ref().map(|s| s.0.as_str())),
            _ => {
                warn!("tried to parse {self:?} as vec string");
                None
            }
        }
    }

    pub fn as_vec_char(&self) -> Option<Vec<Option<char>>> {
        if let Data::Char(s) = self {
            Some(s.iter().map(|o| o.as_deref().copied()).collect())
        } else {
            warn!("tried to parse {self:?} as vec char");
            None
        }
    }

    pub fn as_first_bool(&self) -> Option<bool> {
        if let Data::Bool(b) = self {
            b.iter().find_map(|o| *o)
        } else {
            warn!("tried to parse {self:?} as vec char");
            None
        }
    }

    pub fn to_vec_datetime(&self) -> Option<Vec<Option<OffsetDateTime>>> {
        if let Data::DateTime(s) = self {
            Some(s.iter().map(|o| o.clone().map(|d| *d)).collect())
        } else {
            warn!("tried to parse {self:?} as vec datetime");
            None
        }
    }

    pub fn as_first_u32(&self) -> Option<u32> {
        match self {
            Data::Long(s) => s.first().cloned().flatten().map(|o| *o),
            Data::Short(s) => s.first().cloned().flatten().map(|o| *o),
            _ => {
                warn!("tried to parse {self:?} as first u32");
                None
            }
        }
    }

    pub fn as_vec_u32(&self) -> Option<Vec<Option<u32>>> {
        match self {
            Data::Long(s) => Some(s.iter().map(|o| o.as_ref().map(|s| **s)).collect()),
            Data::Short(s) => Some(s.iter().map(|o| o.as_ref().map(|s| **s)).collect()),
            _ => {
                warn!("tried to parse {self:?} as vec u32");
                None
            }
        }
    }

    pub fn as_vec_u64(&self) -> Option<Vec<Option<u64>>> {
        match self {
            Data::Long(s) => Some(
                s.iter()
                    .map(|o| o.as_ref().map(|s| u64::from(**s)))
                    .collect(),
            ),
            Data::LongLong(s) => Some(s.iter().map(|o| o.as_ref().map(|s| **s)).collect()),
            Data::Short(s) => Some(
                s.iter()
                    .map(|o| o.as_ref().map(|s| u64::from(**s)))
                    .collect(),
            ),
            _ => {
                warn!("tried to parse {self:?} as vec u64");
                None
            }
        }
    }

    pub fn as_vec_f64(&self) -> Option<Vec<Option<f64>>> {
        if let Data::Double(s) = self {
            Some(s.iter().map(|o| o.as_ref().map(|s| **s)).collect())
        } else {
            warn!("tried to parse {self:?} as vec f64");
            None
        }
    }
}

#[derive(Default, Clone)]
pub struct AsciiString(pub String);

impl std::fmt::Debug for AsciiString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_char('"')?;
        f.write_str(&self.0)?;
        f.write_char('"')
    }
}
