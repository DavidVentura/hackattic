use std::error::Error;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct ProblemData {}

pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    Err("asd".into())
}
