use md5;
use serde::Deserialize;
use serde_json::json;
use std::error::Error;
use std::num::ParseIntError;

#[derive(Deserialize, Debug)]
struct ProblemData {
    include: String,
}

pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    assert_eq!(json_data.include.len(), 32);
    let coll_a = decode_hex("d131dd02c5e6eec4693d9a0698aff95c2fcab58712467eab4004583eb8fb7f8955ad340609f4b30283e488832571415a085125e8f7cdc99fd91dbdf280373c5bd8823e3156348f5bae6dacd436c919c6dd53e2b487da03fd02396306d248cda0e99f33420f577ee8ce54b67080a80d1ec69821bcb6a8839396f9652b6ff72a70")?;
    let coll_b = decode_hex("d131dd02c5e6eec4693d9a0698aff95c2fcab50712467eab4004583eb8fb7f8955ad340609f4b30283e4888325f1415a085125e8f7cdc99fd91dbd7280373c5bd8823e3156348f5bae6dacd436c919c6dd53e23487da03fd02396306d248cda0e99f33420f577ee8ce54b67080280d1ec69821bcb6a8839396f965ab6ff72a70")?;

    assert_eq!(coll_a.len(), coll_b.len());
    assert_ne!(coll_a, coll_b);

    let mut f_a = coll_a.clone();
    let mut f_b = coll_b.clone();

    f_a.append(&mut json_data.include.clone().as_bytes().to_vec());
    f_b.append(&mut json_data.include.clone().as_bytes().to_vec());

    assert_ne!(f_a, f_b);

    let m_a: Vec<u8> = md5::compute(&f_a).to_vec();
    let m_b: Vec<u8> = md5::compute(&f_b).to_vec();

    println!("{:?}", m_a);
    println!("{:?}", m_b);

    assert_eq!(m_a, m_b);

    println!("{:?}", json_data.include);
    Ok(json!({ "files": vec![base64::encode(f_a), base64::encode(f_b)] }).to_string())
    //Err("asd".into())
}
fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}
