use sha2::{Digest, Sha256};
use std::error::Error;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
struct Block {
    nonce: Option<()>,
    data: Vec<(String, i32)>,
}
#[derive(Deserialize, Debug)]
struct ProblemData {
    difficulty: u32,
    block: Block,
}

#[derive(Serialize, Debug)]
struct ResponseData {
    data: Vec<(String, i32)>,
    nonce: i32,
}

pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let prob_data: ProblemData = serde_json::from_str(&parsed_data)?;
    println!("{:?}", prob_data);
    assert!(prob_data.difficulty > 8 && prob_data.difficulty < 16); // simplifies things by asserting the result will always fit in 2 bytes
    let mask = 0xff << (16 - prob_data.difficulty);
    println!("Mask: {:b}", mask);

    for nonce in 1..1_000_000 {
        let mut hasher = Sha256::new();
        let attempt = serde_json::to_string(&ResponseData {
            data: prob_data.block.data.clone(),
            nonce,
        })
        .unwrap();
        hasher.update(attempt.clone());

        let bytes = &hasher.finalize()[..];
        let first_two_bytes = u16::from_be_bytes([bytes[0], bytes[1]]);
        if (first_two_bytes ^ mask) == mask {
            println!("first_two_bytes: {:b}", first_two_bytes);
            return Ok(attempt);
        }
    }

    Err("could not find nonce in 1 million attempts, giving up".into())
}
