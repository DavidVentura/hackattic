use serde::Deserialize;
use std::error::Error;

#[derive(Deserialize, Debug)]
struct ProblemResponse {
    bytes: String,
}

pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let response: ProblemResponse = serde_json::from_str(&parsed_data)?;
    let data = base64::decode(response.bytes)?;

    let int = i32::from_le_bytes(data[0..4].try_into()?);
    let uint = u32::from_le_bytes(data[4..8].try_into()?);
    let short = i16::from_le_bytes(data[8..10].try_into()?);

    assert_eq!(i16::from_le_bytes(data[10..12].try_into()?), 0); // 10, 11 are padding zeroes

    let float = f32::from_le_bytes(data[12..16].try_into()?);
    let double = f64::from_le_bytes(data[16..24].try_into()?);
    let big_endian_double = f64::from_be_bytes(data[24..32].try_into()?);

    // Can't use serde_json as it does not keep 14 decimals of precision for float

    let manual_json_str = format!(
        r#"{{"int": {}, "uint": {}, "short": {}, "float": {:.14}, "double": {}, "big_endian_double": {}}}"#,
        int, uint, short, float, double, big_endian_double
    );
    println!("{:?}", manual_json_str);
    Ok(manual_json_str)
}
