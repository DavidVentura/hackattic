mod backup_restore;
mod brute_force_zip;
mod help_me_unpack;
mod mini_miner;
mod redis;
mod serving_dns;

use std::{env, format};

fn main() {
    let token = env::var("HACKATTIC_TOKEN")
        .expect("Expected a token on environment variable HACKATTIC_TOKEN");
    // let problem_name = "the_redis_one";
    let problem_name = "the_redis_one";

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
        //"brute_force_zip" => brute_force_zip::solve(parsed_data),
        "backup_restore" => backup_restore::solve(parsed_data),
        "serving_dns" => serving_dns::solve(parsed_data, solve_problem_url.clone()),
        "the_redis_one" => redis::solve(parsed_data),
        _ => panic!(),
    }
    .unwrap();

    if problem_name != "serving_dns" {
        let submission_result = submit_result(solve_problem_url.as_ref(), &res);
        println!("{:?}", submission_result.unwrap().into_string());
    }
}

fn submit_result(url: &'_ str, res: &'_ str) -> Result<ureq::Response, ureq::Error> {
    ureq::post(url)
        .set("Content-Type", "application/json")
        .send_string(&res)
}
