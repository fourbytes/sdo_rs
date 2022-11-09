#![warn(clippy::pedantic)]
#![feature(int_roundings, let_chains)]
#[macro_use]
extern crate tracing;

use bitflags::bitflags;

pub mod data;
pub mod decode;
pub mod encode;
pub mod fields;
pub mod util;

use data::{AsciiString, Data};
use fields::{
    HAS_MORE_DATA, IS_TEST_DATA, IS_WATCH_UPDATES, MESSAGE_SOURCE, PACKET_FLAG, REQUEST_ID,
    TARGET_ID, TARGET_NAME, WATCH_KEY_INDEX, WATCH_REQUEST_ID, WATCH_TOPIC,
};
use util::generate_request_id;

pub const BROADCAST_UPDATE_ADDRESS: &str = "-1";
pub const BROADCAST_ADDRESS: &str = "-2";

#[derive(Clone, Copy, Debug)]
#[repr(u32)]
pub enum ClientType {
    Undefined = 0,
    Wealth = 1,
    Iress = 2,
    WebIress = 4,
    NetIress = 8,
    WebServices = 16,
    HTMLIress = 32,
    FIX = 64,
    Virtual = 128,
    Mobile = 256,
    RMS = 512,
    IPS = 1024,
    Investor = 2048,
    MarketFeed = 4096,
    Admin = 8192,
    TraderPlus = 16384,
    All = 65535,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum Topic {
    Td1 = 1,
    Td4 = 4,
    Tr12 = 12,
    Td845 = 845,
    Td847 = 847,
    Tr922 = 922,
    Tr970 = 970,
    Tr1684 = 1684,
    Tr1724 = 1724,
    Td1981 = 1981,
    Tr2361 = 2361,
    Tr2939 = 2939,
    Tr2963 = 2963,
    Tr3424 = 3424,
    Tr1748 = 1748,
    Tr1749 = 1749,
    Tr2748 = 2748,
    Td2751 = 2751,
    Tr2753 = 2753,
    Td2756 = 2756,
    TdQuote = 0,
    TdExtPrice = 10,
    TrWatchlist = 14,
    TrLogin = 1012,
    TrLoginInfo = 1013,
    TdLogin = 508,
    TdLoginInfo = 509,
    TdLogout = 2575,
    TdAction = 272,
    TdStartWatch = 1341,
    TrStopWatch = 1110,
    TdMetaRequest = 1851,
    TrMetaRequest = 1852,
    TdError = 406,
    TrForceError = 99999,
    TdMessage = 528,
    TdAlert = 1230,
    TrPing = 1579,
    TdPing = 1580,
    TdIosAccDetail = 482,
    TdIosLogin = 495,
    TdIosGeneral = 984,
    TdControl = 379,
    TdTopDown = 840,
    TdTopUp = 841,
    TdTopVal = 842,
    TdTopVol = 843,
    TdTopupPoint = 2655,
    TdTopdownPoint = 2656,
    TrIosGetPortfolio = 2743,
    TdIosPortfolio = 2746,
    TdIosAlertMessage = 2936,
    TdNewsLibrary = 1736,
    TdExchangeTime = 1282,
    TdIosOrders = 1686,
    UndefinedTopic = -1,
    UserDefinedTopic = -2,
    TdPricestepsGeneral = 2452,
    TrCmdExchList = 3088,
    TdCmdExchList = 3089,
    TrCaf = 3636,
    TdCaf = 3637,
    TdIosPortfolioGroup = 3050,
    TdIosCashManagementTrust = 3095,

    // Manually identified topics
    MExchanges = 3465,
    Unknown351 = 351,
    Unknown484 = 484,
    Unknown614 = 614,
    Unknown1581 = 1581,
    Unknown1687 = 1687,
    Unknown1725Destination = 1725,
    Unknown1868 = 1868,
    Unknown1994 = 1994,
    Unknown2025 = 2025,
    Unknown2324 = 2324,
    Unknown2329 = 2329,
    Unknown2412 = 2412,
    Unknown2465 = 2465,
    Unknown2519 = 2519,
    Unknown2771 = 2771,
    Unknown2777 = 2777,
    Unknown2783 = 2783,
    Unknown2787 = 2787,
    Unknown2781 = 2781,
    Unknown2757 = 2757,
    Unknown2840 = 2840,
    Unknown2872 = 2872,
    Unknown2951 = 2951,
    Unknown2977 = 2977,
    Unknown3097 = 3097,
    Unknown3137 = 3137,
    Unknown3260 = 3260,
    Unknown3314 = 3314,
    Unknown3338 = 3338,
    Unknown3339 = 3339,
    Unknown3348 = 3348,
    Unknown3349 = 3349,
    Unknown3455 = 3455,
    Unknown3468 = 3468,
    Unknown3479 = 3479,
    Unknown3491 = 3491,
    Unknown3530 = 3530,
    Unknown3643 = 3643,
    Unknown3651 = 3651,
    Unknown3751 = 3751,
    // Other(i32),
}

bitflags! {
    #[derive(Debug, Default)]
    pub struct PacketFlags: u32 {
        const CONTINUE = 0;
        const FIRST = 1;
        const LAST = 2;
    }
}

#[derive(Clone, Debug)]
pub struct SDO {
    pub topic: Topic,
    pub fields: Vec<(Field, Option<Data>)>,
}

impl SDO {
    #[must_use]
    pub fn new_with_broadcast_address() -> Self {
        let mut sdo = Self::new(Topic::UndefinedTopic);
        sdo.push_string_w(REQUEST_ID, Some(BROADCAST_ADDRESS.to_string()));
        sdo
    }

    #[must_use]
    pub fn new_with_broadcast_update_address() -> Self {
        let mut sdo = Self::new(Topic::UndefinedTopic);
        sdo.push_string_w(REQUEST_ID, Some(BROADCAST_UPDATE_ADDRESS.to_string()));
        sdo
    }

    /// _Address SDO_: Get the request ID.
    #[must_use]
    pub fn request_id(&self) -> Option<&str> {
        // assert!([Topic::UndefinedTopic].contains(&self.topic));
        self.get_field(REQUEST_ID).and_then(Data::as_first_str)
    }

    /// _Control SDO_: Get the target ID.
    pub fn target_id(&self) -> Option<&str> {
        // assert!([Topic::UndefinedTopic, Topic::TdControl].contains(&self.topic));
        self.get_field(TARGET_ID).and_then(Data::as_first_str)
    }

    /// _Control SDO_: Get the target name.
    pub fn target_name(&self) -> Option<&str> {
        // assert!([Topic::UndefinedTopic, Topic::TdControl].contains(&self.topic));
        self.get_field(TARGET_NAME).and_then(Data::as_first_str)
    }

    /// _Control SDO_: Check if this SDO is test data.
    pub fn is_test_data(&self) -> Option<bool> {
        // assert!([Topic::UndefinedTopic, Topic::TdControl].contains(&self.topic));
        self.get_field(IS_TEST_DATA).and_then(Data::as_first_bool)
    }

    /// _Payload SDO_: Get the packet flags.
    pub fn packet_flag(&self) -> Option<PacketFlags> {
        let is_valid_topic = true; //false;
        if !is_valid_topic && self.topic == Topic::UndefinedTopic {
            error!(
                "Payload SDO is topic of: {:?}, which is unexpected.",
                self.topic
            );
            return None;
        }
        return self
            .get_field(PACKET_FLAG)
            .and_then(|f| f.as_first_u32().and_then(PacketFlags::from_bits));
    }

    #[must_use]
    pub fn has_more_data(&self) -> bool {
        self.get_field(HAS_MORE_DATA)
            .and_then(Data::as_first_u32)
            .map_or(false, |i| i == 1)
    }

    #[must_use]
    pub fn is_last_packet(&self) -> bool {
        self.packet_flag().is_none()
            || self
                .packet_flag()
                .unwrap_or_default()
                .contains(PacketFlags::LAST)
    }

    #[must_use]
    pub fn is_first_packet(&self) -> bool {
        self.packet_flag().is_none()
            || self
                .packet_flag()
                .unwrap_or_default()
                .contains(PacketFlags::FIRST)
    }

    /// _Payload SDO_: Get the message source.
    #[must_use]
    pub fn message_source(&self) -> Option<&str> {
        // assert!([Topic::TdMessage].contains(&self.topic));
        self.get_field(MESSAGE_SOURCE).and_then(Data::as_first_str)
    }

    /// Check if this SDO is watch updates.
    #[must_use]
    pub fn is_watch_updates(&self) -> bool {
        self.get_field(IS_WATCH_UPDATES)
            .and_then(Data::as_first_bool)
            .unwrap_or(false)
    }

    /// _Payload SDO_: Get the watch topic.
    #[must_use]
    pub fn watch_topic(&self) -> Option<u32> {
        // assert!([Topic::TdStartWatch, Topic::TrStopWatch].contains(&self.topic));
        self.get_field(WATCH_TOPIC).and_then(Data::as_first_u32)
    }

    /// Get the watch key index.
    #[must_use]
    pub fn watch_key_index(&self) -> Option<u32> {
        self.get_field(WATCH_KEY_INDEX).and_then(Data::as_first_u32)
    }

    /// Get the watch request id.
    #[must_use]
    pub fn watch_request_id(&self) -> Option<&str> {
        self.get_field(WATCH_REQUEST_ID)
            .and_then(Data::as_first_str)
    }
}

pub trait OneOrMany<T> {
    fn to_vec(self) -> Vec<Option<T>>;
}

impl<T> OneOrMany<T> for Option<T> {
    fn to_vec(self) -> Vec<Option<T>> {
        vec![self]
    }
}

impl<T> OneOrMany<T> for Vec<Option<T>> {
    fn to_vec(self) -> Vec<Option<T>> {
        self
    }
}

impl SDO {
    #[must_use]
    pub fn new(topic: Topic) -> Self {
        Self {
            topic,
            fields: vec![],
        }
    }

    pub fn remove_field(&mut self, id: u32) {
        self.fields
            .retain_mut(|(header, _data)| header.field_id != Some(id));
    }

    #[must_use]
    pub fn get_field(&self, id: u32) -> Option<&Data> {
        self.fields
            .iter()
            .find(|f| f.0.field_id == Some(id))
            .and_then(|f| f.1.as_ref())
    }

    pub fn push_short<T: OneOrMany<u32>>(&mut self, field_id: u32, data: T) {
        let data = data.to_vec();
        self.fields.push((
            Field {
                data_type: DataType::Short,
                extra_info: None,
                field_id: Some(field_id),
                null_flags: None,
                rows: data.len() as u32,
                single_row: data.len() == 1,
                wire_type: WireType::Varint,
            },
            Some(Data::Short(
                data.into_iter().map(|o| o.map(Box::new)).collect(),
            )),
        ));
    }

    pub fn push_sdo<T: OneOrMany<SDO>>(&mut self, field_id: u32, data: T) {
        let data = data.to_vec();
        self.fields.push((
            Field {
                data_type: DataType::SDO,
                extra_info: None,
                field_id: Some(field_id),
                null_flags: None,
                rows: data.len() as u32,
                single_row: data.len() == 1,
                wire_type: WireType::EmbeddedSDO,
            },
            Some(Data::SDO(
                data.into_iter().map(|o| o.map(Box::new)).collect(),
            )),
        ));
    }

    pub fn push_string<T: OneOrMany<String>>(&mut self, field_id: u32, data: T) {
        let data = data.to_vec();
        self.fields.push((
            Field {
                data_type: DataType::String,
                extra_info: None,
                field_id: Some(field_id),
                null_flags: None,
                rows: data.len() as u32,
                single_row: data.len() == 1,
                wire_type: WireType::LengthDelimited,
            },
            Some(Data::AsciiString(
                data.into_iter()
                    .map(|o| o.map(AsciiString).map(Box::new))
                    .collect(),
            )),
        ));
    }

    pub fn push_string_w<T: OneOrMany<String>>(&mut self, field_id: u32, data: T) {
        let data = data.to_vec();
        self.fields.push((
            Field {
                data_type: DataType::StringW,
                extra_info: None,
                field_id: Some(field_id),
                null_flags: None,
                rows: data.len() as u32,
                single_row: data.len() == 1,
                wire_type: WireType::LengthDelimited,
            },
            Some(Data::StringW(
                data.into_iter().map(|o| o.map(Box::new)).collect(),
            )),
        ));
    }

    pub fn push_long<T: OneOrMany<u32>>(&mut self, field_id: u32, data: T) {
        let data = data.to_vec();
        self.fields.push((
            Field {
                data_type: DataType::Long,
                extra_info: None,
                field_id: Some(field_id),
                null_flags: None,
                rows: data.len() as u32,
                single_row: data.len() == 1,
                wire_type: WireType::Varint,
            },
            Some(Data::Long(
                data.into_iter().map(|o| o.map(Box::new)).collect(),
            )),
        ));
    }

    pub fn push_bool<T: OneOrMany<bool>>(&mut self, field_id: u32, data: T) {
        let data = data.to_vec();
        self.fields.push((
            Field {
                data_type: DataType::Boolean,
                extra_info: None,
                field_id: Some(field_id),
                null_flags: None,
                rows: data.len() as u32,
                single_row: data.len() == 1,
                wire_type: WireType::Varint,
            },
            Some(Data::Bool(data)),
        ));
    }
}

#[derive(Debug, Clone)]
pub struct Message {
    pub id: Option<String>,
    pub timeout: Option<String>,
    pub page_size: Option<u32>,
    pub sdo: SDO,
}

impl Message {
    #[must_use]
    pub fn new(topic: Topic) -> Self {
        Self {
            id: Some(generate_request_id()),
            sdo: SDO::new(topic),
            timeout: None,
            page_size: None,
        }
    }

    #[must_use]
    pub fn new_with_id(topic: Topic, id: Option<String>) -> Self {
        Self {
            id,
            sdo: SDO::new(topic),
            timeout: None,
            page_size: None,
        }
    }
}

#[derive(Clone)]
pub struct Field {
    data_type: DataType,
    extra_info: Option<Vec<u8>>,
    field_id: Option<u32>,
    null_flags: Option<Vec<u8>>,
    rows: u32,
    single_row: bool,
    wire_type: WireType,
}

impl std::fmt::Debug for Field {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Field")
            .field("data_type", &self.data_type)
            .field("extra_info", &self.extra_info)
            .field("field_id", &self.field_id)
            // .field("null_flags", &self.null_flags)
            .field("rows", &self.rows)
            // .field("single_row", &self.single_row)
            .field("wire_type", &self.wire_type)
            .finish()
    }
}

impl Field {
    fn new(single_row: bool) -> Self {
        Self {
            data_type: DataType::NoType1,
            extra_info: None,
            field_id: None,
            null_flags: None,
            rows: 1,
            single_row,
            wire_type: WireType::Varint,
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
#[repr(u8)]
enum EncodingVersion {
    BinaryV3 = 7,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum DataType {
    NoType1 = 0,
    String = 1,
    Short = 2,
    Float = 3,
    Double = 4,
    Long = 5,
    Binary = 6,
    Char = 7,
    EncString = 8,
    StringW = 9,
    SDO = 10,
    DateTime = 11,
    LongLong = 12,
    Boolean = 13,
    Unknown,
}

impl From<u8> for DataType {
    fn from(value: u8) -> Self {
        match value {
            0 => DataType::NoType1,
            1 => DataType::String,
            2 => DataType::Short,
            3 => DataType::Float,
            4 => DataType::Double,
            5 => DataType::Long,
            6 => DataType::Binary,
            7 => DataType::Char,
            8 => DataType::EncString,
            9 => DataType::StringW,
            10 => DataType::SDO,
            11 => DataType::DateTime,
            12 => DataType::LongLong,
            13 => DataType::Boolean,
            _ => DataType::Unknown,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum WireType {
    Varint = 0,
    Bit64 = 1,
    LengthDelimited = 2,
    EmbeddedSDO = 3,
    Unknown,
}

impl From<u8> for WireType {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Varint,
            1 => Self::Bit64,
            2 => Self::LengthDelimited,
            3 => Self::EmbeddedSDO,
            _ => Self::Unknown,
        }
    }
}
