use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::error::Error;
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
    U32(u32),
}

#[derive(Debug)]
enum KVPair {
    STR(String, String),
    U8(String, u8),
    U32(String, u32),
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum OpCodes {
    Aux = 0xFA,
    ResizeDB = 0xFB,
    ExpireTimeMs = 0xFC,
    ExpireTimeSec = 0xFD,
    SelectDB = 0xFE,
    EOF = 0xFF,
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum LengthEnc {
    SixBits = 0x0,
    FourteenBits = 0x1,
    FourOrEightBytes = 0x2,
    Encoded = 0x3,
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum SpecialEncoding {
    INT8 = 0x0,
    INT16 = 0x1,
    INT32 = 0x2,
}

fn read_length_encoding<I>(buf: &mut Peekable<I>) -> RedisValue
where
    I: Iterator<Item = u8>,
{
    let first_2_bits = (buf.peek().unwrap() & 0xC0) >> 6;
    let last_6_bits = buf.next().unwrap() & 0x3F;
    let enc = LengthEnc::try_from_primitive(first_2_bits).unwrap();
    match enc {
        LengthEnc::Encoded => {
            let len = SpecialEncoding::try_from_primitive(last_6_bits).unwrap();
            match len {
                SpecialEncoding::INT8 => RedisValue::U8(buf.next().unwrap()),
                SpecialEncoding::INT16 => panic!(),
                SpecialEncoding::INT32 => {
                    let bytes: Vec<u8> = buf.take(4).collect();
                    assert_eq!(bytes.len(), 4);
                    RedisValue::U32(u32::from_le_bytes(bytes.try_into().unwrap()))
                }
            }
        }
        LengthEnc::SixBits => {
            let len = last_6_bits;
            // FIXME: stringy bit
            let val = String::from_utf8(buf.take(len as usize).collect()).unwrap();
            RedisValue::STR(val)
        }
        LengthEnc::FourteenBits => {
            // stringy ?
            let val_len = ((last_6_bits as u32) << 8) | buf.next().unwrap() as u32;
            println!("val len {}", val_len);
            let val = String::from_utf8(buf.take(val_len as usize).collect()).unwrap();
            RedisValue::STR(val)
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

fn read_key_value<I>(buf: &mut Peekable<I>) -> KVPair
where
    I: Iterator<Item = u8>,
{
    let key_len = buf.next().unwrap();
    let key = String::from_utf8(buf.take(key_len as usize).collect()).unwrap();
    match read_length_encoding(buf) {
        RedisValue::STR(v) => KVPair::STR(key, v),
        RedisValue::U8(v) => KVPair::U8(key, v),
        RedisValue::U32(v) => KVPair::U32(key, v),
    }
    // println!("key {}", key);
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
        if *buf.peek().unwrap() != 0xFE {
            return None;
        }
        buf.next(); // consume header
        let db_id = buf.next().unwrap();
        println!("db id {}", db_id);

        loop {
            match OpCodes::try_from(*buf.peek().unwrap()) {
                Ok(OpCodes::ResizeDB) => break,
                _ => panic!(),
            }
        }
        Some(Database {
            id: db_id,
            entries: vec![],
            expiries: vec![],
        })
    }
}
pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    let mut rdb = base64::decode(json_data.rdb).unwrap();
    let _ = json_data.requirements.check_type_of;

    rdb[0] = b'R';
    rdb[1] = b'E';
    rdb[2] = b'D';
    rdb[3] = b'I';
    rdb[4] = b'S';

    assert!(rdb.starts_with("REDIS".as_bytes()));
    rdb = rdb.split_off(9);

    let mut buf = rdb.into_iter().peekable();
    let header = AuxHeader::parse(&mut buf);
    let db = Database::parse(&mut buf);
    let s = Snapshot {
        header: header.unwrap(),
        dbs: vec![db.unwrap()],
    };
    println!("{:#?}", s);
    Err("asd".into())
}
