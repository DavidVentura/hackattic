use num_enum::{IntoPrimitive, TryFromPrimitive};
use regex::Regex;
use serde_json::json;
use std::error::Error;
use std::fs;
use std::iter::Peekable;
use std::time::SystemTime;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Requirements {
    check_type_of: String,
}

#[derive(Deserialize, Debug)]
struct ProblemData {
    rdb: String,
    requirements: Requirements,
}

#[derive(Debug)]
struct AuxHeader {
    entries: Vec<KVPair>,
}

#[derive(Debug)]
struct Snapshot {
    header: AuxHeader,
    dbs: Vec<Database>,
}

#[derive(Debug, Clone)]
struct Database {
    id: u8,
    entries: Vec<KVPair>,
    expiries: Vec<u64>,
}

#[derive(Debug)]
enum Value {
    Raw(u32),
    Encoded(u32),
    U32(u32),
}

#[derive(Debug, Clone)]
enum RedisValue {
    STR(String),
    U8(u8),
    I16(i16),
    U16(u16),
    U32(u32),
    I32(i32),
    I64(i64),
}

#[derive(Debug, Clone)]
struct KVPair {
    key: String,
    val: KVVal,
}

#[derive(Debug, Clone)]
enum KVVal {
    STR(String),
    U8(u8),
    U16(u16),
    U32(u32),
    LIST(Vec<RedisValue>),
}

#[derive(IntoPrimitive, TryFromPrimitive, PartialEq, Debug)]
#[repr(u8)]
enum OpCodes {
    Aux = 0xFA,
    ResizeDB = 0xFB,
    ExpireTimeMs = 0xFC,
    ExpireTimeSec = 0xFD,
    SelectDB = 0xFE,
    EOF = 0xFF,
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug)]
#[repr(u8)]
enum LengthEnc {
    SixBits = 0x0,
    FourteenBits = 0x1,
    FourOrEightBytes = 0x2,
    Encoded = 0x3,
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug)]
#[repr(u8)]
enum SpecialEncoding {
    INT8 = 0x0,
    INT16 = 0x1,
    INT32 = 0x2,
    Compressed = 0x3,
}

#[derive(IntoPrimitive, TryFromPrimitive, Debug)]
#[repr(u8)]
enum ValueTypeEncoding {
    STR = 0,
    /*
     * Ziplist = 10,
     * Intset = 11,
     */
    HashmapZe = 13,
}

fn read_str<I>(buf: &mut Peekable<I>, len: usize) -> String
where
    I: Iterator<Item = u8>,
{
    String::from_utf8(buf.take(len as usize).collect()).unwrap()
}

fn parse_zl_entry<I>(buf: &mut Peekable<I>) -> RedisValue
where
    I: Iterator<Item = u8>,
{
    let _prev_entry_len = match *buf.peek().unwrap() {
        254 => {
            buf.next(); // consume peek
            let _bytes: Vec<u8> = buf.take(4).collect();
            u32::from_le_bytes(_bytes.try_into().unwrap())
        }
        x => {
            buf.next(); // consume peek
            x as u32
        }
    };

    let first_2_bits = (buf.peek().unwrap() & 0xC0) >> 6;
    let second_2_bits = (buf.peek().unwrap() & 0b0011_0000) >> 4;
    let last_6_bits = buf.next().unwrap() & 0x3F;

    match first_2_bits {
        0b00 => RedisValue::STR(read_str(buf, last_6_bits as usize)), // string val with len = 6bits
        0b01 => {
            // string val with len = 14bits
            let len = ((last_6_bits as u16) << 8) | buf.next().unwrap() as u16;
            RedisValue::STR(read_str(buf, len as usize))
        }

        0b10 => {
            // string val with len = 32bits
            RedisValue::STR(read_str(buf, 4))
        }
        0b11 => match second_2_bits {
            0b00 => {
                let _bytes: Vec<u8> = buf.take(2).collect();
                RedisValue::I16(i16::from_le_bytes(_bytes.try_into().unwrap()))
            }
            0b01 => {
                let _bytes: Vec<u8> = buf.take(4).collect();
                RedisValue::I32(i32::from_le_bytes(_bytes.try_into().unwrap()))
            }
            0b10 => {
                let _bytes: Vec<u8> = buf.take(8).collect();
                RedisValue::I64(i64::from_le_bytes(_bytes.try_into().unwrap()))
            }
            0b11 => {
                // i24 ????
                let mut _bytes: Vec<u8> = buf.take(3).collect();
                _bytes.insert(0, 0);
                RedisValue::I32(i32::from_le_bytes(_bytes.try_into().unwrap()))
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

fn parse_ziplist_encoding<I>(buf: &mut Peekable<I>) -> Vec<RedisValue>
where
    I: Iterator<Item = u8>,
{
    let ziplist_len = read_length(buf);

    let _bytes: Vec<u8> = buf.take(4).collect();
    let _zlbytes = u32::from_le_bytes(_bytes.try_into().unwrap());

    let _bytes: Vec<u8> = buf.take(4).collect();
    let _zltail = u32::from_le_bytes(_bytes.try_into().unwrap());

    let _bytes: Vec<u8> = buf.take(2).collect();
    let _zllen = u16::from_le_bytes(_bytes.try_into().unwrap());

    println!("zl len {}", _zllen);
    let ret = (0.._zllen).map(|_| parse_zl_entry(buf)).collect();
    assert_eq!(buf.next().unwrap(), 0xFF); // end of ziplist

    ret
}

fn read_value_type<I>(buf: &mut Peekable<I>) -> KVPair
where
    I: Iterator<Item = u8>,
{
    let _v = buf.next().unwrap();
    let l = read_length(buf);

    let key_len = match l {
        Value::Raw(len) => len as u32,
        Value::U32(len) => len as u32,
        v => panic!("not sure what {:?} is", v),
    };
    let _keybuf: Vec<u8> = buf.take(key_len as usize).collect();
    let key = if let Ok(_key) = String::from_utf8(_keybuf.clone()) {
        _key.to_string()
    } else {
        format!("<<<<key is invalid string {:?}>>>>", _keybuf)
    };

    let val_type = ValueTypeEncoding::try_from_primitive(_v).unwrap();
    KVPair {
        key,
        val: match val_type {
            ValueTypeEncoding::STR => match read_length_encoding(buf) {
                RedisValue::STR(s) => KVVal::STR(s),
                RedisValue::U16(len) => KVVal::U16(len),
                v => panic!("RV {:#?}", v),
            },
            ValueTypeEncoding::HashmapZe => KVVal::LIST(parse_ziplist_encoding(buf)),
        },
    }
}

fn read_length<I>(buf: &mut Peekable<I>) -> Value
where
    I: Iterator<Item = u8>,
{
    let first_2_bits = (buf.peek().unwrap() & 0xC0) >> 6;
    let last_6_bits = buf.next().unwrap() & 0x3F;
    let len = LengthEnc::try_from_primitive(first_2_bits).unwrap();
    match len {
        LengthEnc::Encoded => Value::Encoded(last_6_bits as u32),

        LengthEnc::SixBits => Value::Raw(last_6_bits as u32),

        LengthEnc::FourteenBits => {
            Value::Raw(((last_6_bits as u32) << 8) | buf.next().unwrap() as u32)
        }
        LengthEnc::FourOrEightBytes => {
            let len_discriminator = buf.next().unwrap();
            if len_discriminator == 0 {
                // 32 bit, "net order"
                let bytes: Vec<u8> = buf.take(4).collect();
                Value::U32(u32::from_be_bytes(bytes.try_into().unwrap()))
                //RedisValue::U32(u32::from_be_bytes(bytes.try_into().unwrap()))
            } else {
                // 64 bit, "net order"
                assert_eq!(len_discriminator, 1);
                panic!();
                // RedisValue::U64(u64::from_be(bytes))
            }
        }
    }
}
fn read_length_encoding<I>(buf: &mut Peekable<I>) -> RedisValue
where
    I: Iterator<Item = u8>,
{
    match read_length(buf) {
        Value::Encoded(last_6_bits) => {
            let enc = SpecialEncoding::try_from_primitive(last_6_bits as u8).unwrap();
            match enc {
                SpecialEncoding::INT8 => RedisValue::U8(buf.next().unwrap()),
                SpecialEncoding::INT16 => {
                    let bytes: Vec<u8> = buf.take(2).collect();
                    assert_eq!(bytes.len(), 2);
                    RedisValue::U16(u16::from_le_bytes(bytes.try_into().unwrap()))
                }
                SpecialEncoding::INT32 => {
                    let bytes: Vec<u8> = buf.take(4).collect();
                    assert_eq!(bytes.len(), 4);
                    RedisValue::U32(u32::from_le_bytes(bytes.try_into().unwrap()))
                }
                SpecialEncoding::Compressed => panic!(),
            }
        }
        Value::Raw(bytes) => {
            RedisValue::STR(String::from_utf8(buf.take(bytes as usize).collect()).unwrap())
        }
        Value::U32(num) => RedisValue::U32(num),
    }
}

fn read_key_value<I>(buf: &mut Peekable<I>) -> KVPair
where
    I: Iterator<Item = u8>,
{
    let key_len = buf.next().unwrap();
    let key = String::from_utf8(buf.take(key_len as usize).collect()).unwrap();
    KVPair {
        key,
        val: match read_length_encoding(buf) {
            RedisValue::STR(v) => KVVal::STR(v),
            RedisValue::U8(v) => KVVal::U8(v),
            RedisValue::U16(v) => KVVal::U16(v),
            RedisValue::U32(v) => KVVal::U32(v),
            _ => panic!(),
        },
    }
}

impl AuxHeader {
    fn parse<I>(buf: &mut Peekable<I>) -> Option<AuxHeader>
    where
        I: Iterator<Item = u8>,
    {
        let mut entries = vec![];
        loop {
            match OpCodes::try_from(*buf.peek().unwrap()) {
                Ok(OpCodes::SelectDB) => break, // AuxHeader ends on SelectDB
                Ok(OpCodes::Aux) => {
                    buf.next(); // consume the peek'd position
                    let ver = read_key_value(buf);
                    entries.push(ver);
                }
                Ok(_) => panic!("something else"),
                Err(_) => panic!("ran out"),
            }
        }
        Some(AuxHeader { entries })
    }
}

impl Database {
    fn parse<I>(buf: &mut Peekable<I>) -> Option<Database>
    where
        I: Iterator<Item = u8>,
    {
        if OpCodes::try_from(*buf.peek().unwrap()) != Ok(OpCodes::SelectDB) {
            return None;
        }
        buf.next(); // consume opcode
        let db_id = buf.next().unwrap();

        assert_eq!(
            OpCodes::try_from(buf.next().unwrap()).unwrap(),
            OpCodes::ResizeDB
        );
        let hash_size = if let Value::Raw(len) = read_length(buf) {
            len
        } else {
            panic!();
        };
        let expire_size = if let Value::Raw(len) = read_length(buf) {
            len
        } else {
            panic!();
        };

        let mut entries: Vec<KVPair> = Vec::with_capacity(hash_size as usize);
        let mut expiries: Vec<u64> = Vec::with_capacity(expire_size as usize);
        let mut last_expiry: u64 = 0;
        loop {
            let opcode = *buf.peek().unwrap();
            match OpCodes::try_from(opcode) {
                Ok(OpCodes::SelectDB) => break, // DB ends on SelectDB
                Ok(OpCodes::EOF) => break,      // File ends on SelectDB
                Ok(OpCodes::ExpireTimeMs) => {
                    buf.next().unwrap(); // consume opcode
                    let data: Vec<u8> = buf.take(8).collect();
                    let expiry_ms = u64::from_le_bytes(data.try_into().unwrap());
                    last_expiry = expiry_ms;
                }
                Ok(unkn) => panic!("Unknown OpCode {:#?}", unkn),
                Err(_) if opcode < 245 => {
                    let e = read_value_type(buf);
                    expiries.push(last_expiry);
                    last_expiry = 0;
                    entries.push(e);
                }
                Err(_) => panic!("Unknown OpCode {:#?}", opcode),
            }
        }
        assert_eq!(entries.len() as u32, hash_size);
        Some(Database {
            id: db_id,
            entries,
            expiries: expiries,
        })
    }
}
pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    let mut rdb = base64::decode(json_data.rdb).unwrap();
    let key_to_check = json_data.requirements.check_type_of;

    rdb[0] = b'R';
    rdb[1] = b'E';
    rdb[2] = b'D';
    rdb[3] = b'I';
    rdb[4] = b'S';

    fs::write(
        format!(
            "db-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .as_secs()
        ),
        rdb.clone(),
    )?;
    /*
    assert!(rdb.starts_with("REDIS".as_bytes()));
    let rdb = fs::read("snap")?[9..].to_vec();
    */
    let buf = &mut rdb.into_iter().peekable();
    let _: Vec<u8> = buf.take(9).collect(); // header

    let header = AuxHeader::parse(buf);
    let mut dbs = vec![];
    while let Some(d) = Database::parse(buf) {
        dbs.push(d);
    }

    let s = Snapshot {
        header: header.unwrap(),
        dbs: dbs.clone(),
    };
    println!("{:#?}", s);

    let db_count = dbs.len();
    let mut emoji_key_value: String = "???".to_string();
    let mut expiry_millis = 0;
    let mut type_of_key_to_check = "type of key to check";

    let emoji_regex = Regex::new(r"\p{Emoji}").unwrap();

    println!("need to check type of {}", key_to_check);
    for db in dbs {
        for expiry in db.expiries {
            if expiry > 0 {
                assert_eq!(expiry_millis, 0);
                expiry_millis = expiry;
            }
        }
        for entry in db.entries {
            if emoji_regex.is_match(&entry.key) {
                println!("This key is emoji: {} {:?}", entry.key, entry.val);
                if let KVVal::STR(s) = entry.val.clone() {
                    emoji_key_value = s;
                } else {
                    panic!("idk how to store non-string val");
                }
            }

            if entry.key == key_to_check {
                type_of_key_to_check = match entry.val {
                    KVVal::U8(val) => "number",
                    KVVal::U16(val) => "number",
                    KVVal::U32(val) => "number",
                    KVVal::STR(val) => "string",
                    KVVal::LIST(val) => "hash",
                }
            }
        }
    }

    let res = json!({"db_count": db_count, "emoji_key_value": emoji_key_value, "expiry_millis": expiry_millis, key_to_check: type_of_key_to_check}).to_string();
    println!("Submitting {}", res);
    Ok(res)
}
