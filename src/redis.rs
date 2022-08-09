//use byteorder::ReadBytesExt;
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

struct AuxHeader {
    ver: String,
}

#[derive(Debug)]
enum RedisValues {
    STR(String, String),
    U8(String, u8),
    U32(String, u32),
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
    match buf.peek() {
        Some(0xFA) => {
            buf.next(); // consume the peek'd position
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
                    let val_len =
                        (((buf.next().unwrap() & 0x3F) as u32) << 8) | buf.next().unwrap() as u32;
                    println!("val len {}", val_len);
                    let val = String::from_utf8(buf.take(val_len as usize).collect()).unwrap();
                    RedisValues::STR(key, val)
                }
                _ => panic!(),
            }
        }
        _ => panic!("idk what to do"),
    }
}

impl AuxHeader {
    fn parse<I>(buf: &mut Peekable<I>) -> Option<AuxHeader>
    where
        I: Iterator<Item = u8>,
    {
        loop {
            match buf.peek() {
                Some(0xFE) => break,
                Some(val) => {
                    let ver = read_key_value(buf);
                    println!("{:?}", ver);
                }
                None => panic!("ran out"),
            }
        }
        None
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

    AuxHeader::parse(&mut rdb.into_iter().peekable());
    Err("asd".into())
}
