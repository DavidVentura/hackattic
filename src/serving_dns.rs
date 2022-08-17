use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use serde_json::json;
use std::net::{Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::thread;
use std::time::Instant;
use std::{error::Error, net::UdpSocket};

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct Entry {
    name: String,
    #[serde(rename = "type")]
    etype: String,
    data: String,
}

#[derive(Deserialize, Debug)]
struct ProblemData {
    records: Vec<Entry>,
}

#[derive(FromPrimitive, Debug, Clone, Copy)]
enum QuestionClass {
    IN = 1,
}

#[derive(FromPrimitive, Debug, Clone, Copy, PartialEq)]
enum QuestionType {
    A = 1,
    CNAME = 5,
    AAAA = 28,
    RP = 17,
    TXT = 16,
}

#[derive(FromPrimitive, Debug, Clone, Copy)]
enum QueryType {
    Query = 0,
    Reply,
}

#[derive(FromPrimitive, Debug, Clone, Copy)]
enum Opcode {
    Query = 0,
    IQuery,
    Status,
}

#[derive(FromPrimitive, Debug, Clone, Copy)]
enum ResponseCode {
    NoError = 0,
    FormatError,
    ServFail,
    NxDomain,
    // ...
}

#[derive(Debug, Clone, Copy)]
struct Flags {
    #[allow(dead_code)]
    query_type: QueryType, // query (0) || reply (1)
    opcode: Opcode, // query (0) || iquery (1) || status (2)
    // authoritative_answer: bool,
    #[allow(dead_code)]
    truncated: bool,
    recursion_desired: bool,
    #[allow(dead_code)]
    ad_bit: bool,
    #[allow(dead_code)]
    unauthenticated_ok: bool,
    #[allow(dead_code)]
    response_code: ResponseCode,
}

impl Flags {
    fn from_bytes(b: &'_ [u8]) -> Flags {
        Flags {
            query_type: QueryType::from_u8(b[0] & 0b1000_0000).unwrap(),
            opcode: Opcode::from_u8(b[0] & 0b0111_1000).unwrap(),
            // blank ?
            truncated: b[0] & 0b0000_0010 != 0,
            recursion_desired: b[0] & 0b0000_0001 != 0,
            // blank first 2 bits
            ad_bit: b[1] & 0b0010_0000 != 0,
            unauthenticated_ok: b[1] & 0b0001_0000 != 0,
            // not part of request
            response_code: ResponseCode::from_u8(b[1] & 0b0000_1111).unwrap(),
        }
    }
    fn to_bytes(&self, r: ResponseCode) -> Vec<u8> {
        let v = ((QueryType::Reply as u16) << 15) | ((self.opcode as u16) << 11) | // skip AA and truncation == always 0
            ((self.recursion_desired as u16) << 8) |// << 8 is recursion available, but that's also 0
            (r as u16);
        // z is also 0
        let res = v.to_be_bytes().to_vec();
        res
    }
}

#[derive(Debug)]
struct Message {
    identification: u16, // TBD
    flags: Flags,
    question_len: u16,
    answer_len: u16,
    #[allow(dead_code)]
    auth_rr_len: u16,
    #[allow(dead_code)]
    additional_rr_len: u16,
}

impl Message {
    fn from_bytes(b: &'_ [u8]) -> Message {
        Message {
            identification: u16::from_be_bytes([b[0], b[1]]),
            flags: Flags::from_bytes(&b[2..4]),
            question_len: u16::from_be_bytes([b[4], b[5]]),
            answer_len: u16::from_be_bytes([b[6], b[7]]),
            auth_rr_len: u16::from_be_bytes([b[8], b[9]]),
            additional_rr_len: u16::from_be_bytes([b[10], b[11]]),
        }
    }
    fn to_bytes(&self, r: ResponseCode) -> Vec<u8> {
        let mut response: Vec<u8> = Vec::with_capacity(4);
        response.append(&mut self.identification.to_be_bytes().to_vec());
        response.append(&mut self.flags.to_bytes(r));
        response.append(&mut self.question_len.to_be_bytes().to_vec());
        response.append(&mut self.answer_len.to_be_bytes().to_vec());
        response.push(0);
        response.push(self.auth_rr_len as u8);
        response.push(0);
        response.push(self.additional_rr_len as u8);
        response
    }
}

#[derive(Debug, Clone)]
struct Question {
    domain: String,
    qtype: QuestionType,
    class: QuestionClass,
}

fn data_as_sized_labels(buf: &'_ str) -> Vec<u8> {
    let mut ret = Vec::with_capacity(16);
    for part in buf.split(".") {
        let size = part.len() as u8;
        if size > 0 {
            ret.push(size);
            ret.append(&mut part.as_bytes().to_vec());
        }
    }
    ret.push(0);
    ret
}
impl Question {
    fn from_bytes(mut buf: &mut [u8]) -> Question {
        let mut labels: Vec<String> = Vec::with_capacity(2);
        loop {
            let label_len: usize = buf[0] as usize;
            if label_len == 0 {
                break;
            }
            let end: usize = label_len as usize + 1;
            let label = &buf[1..end];
            labels.push(String::from_utf8(label.to_vec()).unwrap());
            buf = &mut buf[label_len + 1..];
        }

        buf = &mut buf[1..];
        let qtype = QuestionType::from_u16(u16::from_be_bytes([buf[0], buf[1]])).unwrap();
        let class = QuestionClass::from_u16(u16::from_be_bytes([buf[2], buf[3]])).unwrap();
        // FIXME: should run this but doesn't mutate outside buffer
        // buf = &mut buf[4..];

        Question {
            domain: labels.join("."),
            qtype,
            class,
        }
    }
    fn to_bytes(&self) -> Vec<u8> {
        let mut response = Vec::with_capacity(16);
        for label in self.domain.split(".") {
            response.push(label.len() as u8);
            response.append(&mut label.as_bytes().to_vec());
        }
        response.push(0); // end
        response.append(&mut (self.qtype as u16).to_be_bytes().to_vec());
        response.append(&mut (self.class as u16).to_be_bytes().to_vec());
        response
    }
}

#[derive(Debug)]
struct Answer {
    name: String,
    atype: QuestionType,
    class: QuestionClass,
    ttl: u32,
    data: Vec<u8>,
    glob: bool,
}

impl Answer {
    fn to_bytes(&self, m: &'_ Message, q: &'_ Question, r: ResponseCode) -> Vec<u8> {
        let mut response: Vec<u8> = Vec::with_capacity(16);
        response.append(&mut m.to_bytes(r));
        response.append(&mut q.to_bytes());
        // TODO send message ))
        response.push(0xc0); // marker for offset ??
        response.push(0x0c); // hardcoded 12 bytes from start, points to the first name on the query
                             // these are _not_ necessarily correct
        response.append(&mut (self.atype as u16).to_be_bytes().to_vec());
        response.append(&mut (self.class as u16).to_be_bytes().to_vec());
        response.append(&mut self.ttl.to_be_bytes().to_vec());
        if q.qtype == QuestionType::TXT {
            // data len
            response.append(&mut u16::to_be_bytes((self.data.len() + 1) as u16).to_vec());
            // txt len TODO: deal with >255 len
            response.push(self.data.len() as u8);
            response.append(self.data.clone().as_mut());
        } else if q.qtype == QuestionType::RP {
            let mut email = data_as_sized_labels("");
            let mut data =
                data_as_sized_labels(String::from_utf8_lossy(&self.data).to_owned().as_ref());
            response.append(&mut u16::to_be_bytes(email.len() as u16 + data.len() as u16).to_vec());
            response.append(&mut email);
            response.append(&mut data);
        } else {
            response.append(&mut u16::to_be_bytes(self.data.len() as u16).to_vec());
            response.append(self.data.clone().as_mut());
        }
        response
    }
}

pub fn solve(parsed_data: String, url: String) -> Result<String, Box<dyn Error>> {
    let mut answers: Vec<Answer> = vec![];
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;

    for e in json_data.records {
        println!("{:?}", e);
        let glob = e.name.clone().starts_with("*");
        answers.push(Answer {
            name: if glob {
                // drop the `*.` prefix
                e.name.clone()[2..].to_owned()
            } else {
                e.name.clone()
            },
            data: match e.etype.as_str() {
                "A" => Ipv4Addr::from_str(e.data.as_str())
                    .unwrap()
                    .octets()
                    .to_vec(),
                "AAAA" => Ipv6Addr::from_str(e.data.as_str())
                    .unwrap()
                    .octets()
                    .to_vec(),
                "RP" => e.data.into_bytes(),
                "TXT" => e.data.into_bytes(),
                _ => panic!(),
            },
            atype: match e.etype.as_str() {
                "A" => QuestionType::A,
                "AAAA" => QuestionType::AAAA,
                "RP" => QuestionType::RP,
                "TXT" => QuestionType::TXT,
                _ => panic!(),
            },
            class: QuestionClass::IN,
            ttl: 999,
            glob,
        });
    }

    let socket = UdpSocket::bind("0.0.0.0:15353")?;
    let handler = thread::spawn(move || {
        let r = crate::submit_result(
            url.as_ref(),
            json!({"dns_ip": "78.46.233.60", "dns_port": 15353u32})
                .to_string()
                .as_ref(),
        );
        println!("{:?}", r.unwrap().into_string());
    });
    loop {
        println!("Waiting for DNS req..");
        let mut buf = [0; 1440];
        let (_, addr) = socket.recv_from(&mut buf)?;
        let start = Instant::now();
        //println!("{:?}", &addr);
        //println!("{:?} {}", &buf[..len], len);
        let mut message = Message::from_bytes(&buf[..12]);
        message.additional_rr_len = 0;
        message.answer_len = 1;
        //println!("{:?}", message);
        assert_eq!(message.question_len, 1);
        let q = Question::from_bytes(&mut buf[12..]);
        // println!("{:?}", q);

        let mut found = false;
        for a in &answers {
            if a.name != q.domain && (!a.glob || !q.domain.ends_with(a.name.as_str())) {
                // println!("Request domain {} is not {}", q.domain, a.name);
                continue;
            }
            if a.atype != q.qtype {
                // println!("Request type {:?} is not {:?}", q.qtype, a.atype);
                continue;
            }
            socket.send_to(&a.to_bytes(&message, &q, ResponseCode::NoError), addr)?;
            found = true;
            break;
        }
        if !found {
            println!("Not found!");
            socket.send_to(
                &Answer {
                    name: q.domain.clone(),
                    atype: q.qtype,
                    class: q.class,
                    ttl: 1,
                    data: vec![],
                    glob: false,
                }
                .to_bytes(&message, &q, ResponseCode::NxDomain),
                addr,
            )?;
        }
        println!("Took {:?} to send", Instant::now() - start);
    }
}
