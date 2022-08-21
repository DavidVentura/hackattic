use regex::Regex;
use serde::Deserialize;
use serde_json::json;
use std::error::Error;
use std::time;
use tungstenite::{connect, Message};
use url::Url;

#[derive(Deserialize, Debug)]
struct ProblemData {
    token: String,
}

pub fn solve(parsed_data: String) -> Result<String, Box<dyn Error>> {
    let intervals = vec![700, 1500, 2000, 2500, 3000];

    let json_data: ProblemData = serde_json::from_str(&parsed_data)?;
    let re = Regex::new(r#"congratulations! the solution to this challenge is "(.*)""#).unwrap();
    // TODO assert ok
    let m = re.captures(
        r#"congratulations! the solution to this challenge is "muddy art winter recipe aged lab""#,
    ).unwrap();
    assert_eq!(m[1], "muddy art winter recipe aged lab".to_owned());

    let url = format!("wss://hackattic.com/_/ws/{}", json_data.token);
    println!("URL is {}", url);
    let (mut socket, _) = connect(Url::parse(&url)?)?;

    let mut last_msg = time::Instant::now();
    loop {
        let m = socket.read_message()?;
        println!("{}", m);
        if let Ok(text) = m.into_text() {
            match text.as_str() {
                "good!" => (),
                "ping!" => {
                    let now = time::Instant::now();
                    let elapsed = now - last_msg;
                    last_msg = now;
                    let calc = closes_to(elapsed.as_millis().try_into()?, &intervals);
                    println!("Elapsed {:?}, calc {}", elapsed, calc);
                    let reply = format!("{}", calc);
                    socket.write_message(Message::Text(reply))?;
                }
                hello if hello.starts_with("hello") => println!("Hello msg: {}", hello),
                win if win.starts_with("congratulations") => {
                    println!("Win msg: {}", win);
                    let secret = &re.captures(win).unwrap()[1];
                    println!("secret: {}", secret);
                    return Ok(json!({ "secret": secret.to_string() }).to_string());
                }
                unk => println!("Unknown msg: {}", unk),
            }
        }
    }
}

fn closes_to(millis: i32, values: &[i32]) -> i32 {
    let mut delta = millis;
    let mut result = values[0];
    for item in values {
        if (item - millis).abs() < delta {
            result = *item;
            delta = (item - millis).abs();
        }
    }
    result
}
