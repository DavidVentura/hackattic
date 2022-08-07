mod help_me_unpack;
mod mini_miner;

use std::{env, format};

fn main() {
    let token = env::var("HACKATTIC_TOKEN")
        .expect("Expected a token on environment variable HACKATTIC_TOKEN");
    let problem_name = "mini_miner";

    let problem_url = format!(
        "https://hackattic.com/challenges/{}/problem?access_token={}",
        problem_name, token
    );
    let solve_problem_url = format!(
        "https://hackattic.com/challenges/{}/solve?access_token={}",
        problem_name, token
    );

    let problem_data = ureq::get(&problem_url)
        .call()
        .expect("Could not fetch problem");

    let parsed_data = problem_data.into_string().unwrap();

    let res = match problem_name {
        "help_me_unpack" => help_me_unpack::solve(parsed_data),
        "mini_miner" => mini_miner::solve(parsed_data),
        _ => panic!(),
    }
    .unwrap();

    let submission_result = ureq::post(&solve_problem_url)
        .set("Content-Type", "application/json")
        .send_string(&res);
    println!("{:?}", submission_result.unwrap().into_string());
}
