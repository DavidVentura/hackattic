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
    entries: Vec<RedisValues>,
}

#[derive(Debug)]
struct Snapshot {
    header: AuxHeader,
    dbs: Vec<Database>,
}

#[derive(Debug)]
struct Database {
    id: u8,
    entries: Vec<RedisValues>,
    expiries: Vec<u32>,
}

#[derive(Debug)]
enum RedisValues {
    STR(String, String),
    U8(String, u8),
    U32(String, u32),
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum OpCodes {
    Aux = 0xFA,
    SelectDB = 0xFE,
    ResizeDB = 0xFB,
}
const RDB_ENCVAL: u8 = 0x3;

const RDB_6BITLEN: u8 = 0x0;
const RDB_14BITLEN: u8 = 0x1;

const RDB_ENC_INT8: u8 = 0x0;
const RDB_ENC_INT16: u8 = 0x1;
const RDB_ENC_INT32: u8 = 0x2;

fn read_key_value<I>(buf: &mut Peekable<I>) -> RedisValues
where
    I: Iterator<Item = u8>,
{
    let key_len = buf.next().unwrap();
    let key = String::from_utf8(buf.take(key_len as usize).collect()).unwrap();
    // println!("key {}", key);
    let hint = (buf.peek().unwrap() & 0xC0) >> 6;
    // println!("peek {} hint {}", buf.peek().unwrap(), hint);
    match hint {
        RDB_ENCVAL => {
            let len = buf.next().unwrap() & 0x3F; // last 6 bits
                                                  // isencoded now
            match len {
                RDB_ENC_INT8 => RedisValues::U8(key, buf.next().unwrap()),
                RDB_ENC_INT16 => panic!(),
                RDB_ENC_INT32 => {
                    let bytes: Vec<u8> = buf.take(4).collect();
                    assert_eq!(bytes.len(), 4);
                    RedisValues::U32(
                        key,
                        u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
                    )
                }
                _ => panic!(),
            }
        }
        RDB_6BITLEN => {
            let len = buf.next().unwrap() & 0x3F; // last 6 bits
                                                  // RedisValues::U8(buf.next().unwrap())
                                                  // FIXME: stringy bit
            let val = String::from_utf8(buf.take(len as usize).collect()).unwrap();
            RedisValues::STR(key, val)
        }
        RDB_14BITLEN => {
            // stringy ?
            let val_len = (((buf.next().unwrap() & 0x3F) as u32) << 8) | buf.next().unwrap() as u32;
            println!("val len {}", val_len);
            let val = String::from_utf8(buf.take(val_len as usize).collect()).unwrap();
            RedisValues::STR(key, val)
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
