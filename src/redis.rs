use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::error::Error;
use std::fs;
use std::iter::Peekable;

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

#[derive(Debug)]
struct Database {
    id: u8,
    entries: Vec<KVPair>,
    expiries: Vec<u32>,
}

#[derive(Debug)]
enum RedisValue {
    STR(String),
    U8(u8),
    I16(i16),
    U16(u16),
    U32(u32),
    I32(i32),
    I64(i64),
    UNKNOWN_TYPE(u32),
}

#[derive(Debug)]
enum KVPair {
    STR(String, String),
    U8(String, u8),
    U16(String, u16),
    U32(String, u32),
    LIST(String, Vec<RedisValue>),
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
    let ziplist_len = read_length_encoding(buf);

    let _bytes: Vec<u8> = buf.take(4).collect();
    let _zlbytes = u32::from_le_bytes(_bytes.try_into().unwrap());

    let _bytes: Vec<u8> = buf.take(4).collect();
    let _zltail = u32::from_le_bytes(_bytes.try_into().unwrap());

    let _bytes: Vec<u8> = buf.take(2).collect();
    let _zllen = u16::from_le_bytes(_bytes.try_into().unwrap());

    println!("{}", _zllen);
    let ret = (0.._zllen).map(|_| parse_zl_entry(buf)).collect();
    assert_eq!(buf.next().unwrap(), 0xFF); // end of ziplist

    ret
}

fn read_value_type<I>(buf: &mut Peekable<I>) -> KVPair
where
    I: Iterator<Item = u8>,
{
    let _v = buf.next().unwrap();
    let key_len = match read_length_encoding(buf) {
        RedisValue::UNKNOWN_TYPE(len) => len,
        _ => panic!(),
    };
    let key = String::from_utf8(buf.take(key_len as usize).collect()).unwrap();
    println!("got key from valuetype {}", key);

    let val_type = ValueTypeEncoding::try_from_primitive(_v).unwrap();
    match val_type {
        ValueTypeEncoding::STR => match read_length_encoding(buf) {
            RedisValue::UNKNOWN_TYPE(len) => KVPair::STR(
                key,
                String::from_utf8(buf.take(len as usize).collect()).unwrap(),
            ),
            RedisValue::U16(len) => KVPair::U16(key, len),
            _ => panic!(),
        },
        ValueTypeEncoding::HashmapZe => KVPair::LIST(key, parse_ziplist_encoding(buf)),
    }
}

fn read_length_encoding<I>(buf: &mut Peekable<I>) -> RedisValue
where
    I: Iterator<Item = u8>,
{
    let first_2_bits = (buf.peek().unwrap() & 0xC0) >> 6;
    let last_6_bits = buf.next().unwrap() & 0x3F;
    let len = LengthEnc::try_from_primitive(first_2_bits).unwrap();
    match len {
        LengthEnc::Encoded => {
            let enc = SpecialEncoding::try_from_primitive(last_6_bits).unwrap();
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
        LengthEnc::SixBits => {
            //panic!();
            let len = last_6_bits;
            // FIXME: how to decide the type? i assume it's always STR otherwise it'd use the
            // special encoding
            println!("reading 6bit encoding: {}", len);
            RedisValue::UNKNOWN_TYPE(len as u32)
        }
        LengthEnc::FourteenBits => {
            // stringy ?
            let val_len = ((last_6_bits as u16) << 8) | buf.next().unwrap() as u16;
            println!("14bit val len {}", val_len);
            //let val = String::from_utf8(buf.take(val_len as usize).collect()).unwrap();
            //RedisValue::STR(val)
            RedisValue::U16(val_len)
        }
        LengthEnc::FourOrEightBytes => {
            let len_discriminator = buf.next().unwrap();
            if len_discriminator == 0 {
                // 32 bit, "net order"
                let bytes: Vec<u8> = buf.take(4).collect();
                RedisValue::U32(u32::from_be_bytes(bytes.try_into().unwrap()))
            } else {
                // 64 bit, "net order"
                assert_eq!(len_discriminator, 1);
                panic!();
                // RedisValue::U64(u64::from_be(bytes))
            }
        }
    }
}

fn read_u32_le<I>(buf: &mut Peekable<I>, len: u32) -> u32
where
    I: Iterator<Item = u8>,
{
    let bytes: Vec<u8> = buf.take(len as usize).collect();
    match len {
        0 => 0u32,
        1 => bytes[0] as u32,
        2 => u16::from_le_bytes(bytes.try_into().unwrap()) as u32,
        4 => u32::from_le_bytes(bytes.try_into().unwrap()),
        v => panic!("unsure how to read u32 of len {}", v),
    }
}

fn read_key_value<I>(buf: &mut Peekable<I>) -> KVPair
where
    I: Iterator<Item = u8>,
{
    let key_len = buf.next().unwrap();
    let key = String::from_utf8(buf.take(key_len as usize).collect()).unwrap();
    match read_length_encoding(buf) {
        RedisValue::STR(v) => KVPair::STR(key, v),
        RedisValue::U8(v) => KVPair::U8(key, v),
        RedisValue::U16(v) => KVPair::U16(key, v),
        RedisValue::U32(v) => KVPair::U32(key, v),
        RedisValue::UNKNOWN_TYPE(len) => {
            let val = String::from_utf8(buf.take(len as usize).collect()).unwrap();
            KVPair::STR(key, val)
        }
        _ => panic!(),
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
        println!("fetching DB id");
        let db_id = buf.next().unwrap();
        println!("db id {}", db_id);

        assert_eq!(
            OpCodes::try_from(buf.next().unwrap()).unwrap(),
            OpCodes::ResizeDB
        );
        let hash_size = if let RedisValue::UNKNOWN_TYPE(len) = read_length_encoding(buf) {
            len
        } else {
            999
        };
        let expire_size = if let RedisValue::UNKNOWN_TYPE(len) = read_length_encoding(buf) {
            len
        } else {
            999
        };

        println!("hash {:?} expire {:?}", hash_size, expire_size);
        let mut entries: Vec<KVPair> = Vec::with_capacity(hash_size as usize);
        loop {
            match OpCodes::try_from(*buf.peek().unwrap()) {
                Ok(OpCodes::SelectDB) => break,  // DB ends on SelectDB
                Ok(OpCodes::EOF) => return None, // File ends on SelectDB
                Ok(_) => panic!(),
                Err(_) => entries.push(read_value_type(buf)),
            }
        }
        Some(Database {
            id: db_id,
            entries,
            expiries: vec![],
        })
    }
}
pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    /*
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    let mut rdb = base64::decode(json_data.rdb).unwrap();
    let _ = json_data.requirements.check_type_of;

    rdb[0] = b'R';
    rdb[1] = b'E';
    rdb[2] = b'D';
    rdb[3] = b'I';
    rdb[4] = b'S';

    assert!(rdb.starts_with("REDIS".as_bytes()));
    */
    let rdb = fs::read("snap")?[9..].to_vec();
    let mut buf = rdb.into_iter().peekable();
    //buf.take(9); // header

    let header = AuxHeader::parse(&mut buf);
    let mut dbs = vec![];
    for _ in 0..4 {
        if let Some(d) = Database::parse(&mut buf) {
            dbs.push(d);
        }
    }

    let s = Snapshot {
        header: header.unwrap(),
        dbs,
    };
    println!("{:#?}", s);
    Err("asd".into())
}
