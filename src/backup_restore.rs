use flate2::read::GzDecoder;
use std::error::Error;
use std::io::prelude::*;

use serde::{Deserialize, Serialize};

#[derive(Deserialize, Debug)]
struct ProblemData {
    dump: String,
}

#[derive(Serialize, Debug)]
struct ProblemResponse {
    alive_ssns: Vec<String>,
}

pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    let compressed_data = base64::decode(json_data.dump)?;
    let mut decoder = GzDecoder::new(compressed_data.as_slice());
    let mut uncompressed_data = String::new();
    decoder.read_to_string(&mut uncompressed_data).unwrap();

    let mut seen_copy = false;
    let mut alive_ssns: Vec<String> = vec![];
    for line in uncompressed_data.lines() {
        if line.starts_with("COPY") {
            seen_copy = true;
            continue;
        }
        if !seen_copy {
            continue;
        }
        if line.starts_with(r#"\."#) {
            // Delimiter for "end of block"
            break;
        }
        let fields: Vec<&str> = line.split("\t").collect();
        let ssn = fields.get(3).unwrap();
        let alive = fields.get(7).unwrap();
        if alive.to_string() == "alive" {
            alive_ssns.push(ssn.to_string());
        }
    }
    Ok(serde_json::to_string(&ProblemResponse { alive_ssns })?)
}
