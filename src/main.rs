use pest::Parser;
use std::fs;

#[derive(pest_derive::Parser)]
#[grammar = "sql.pest"]
struct SqlParser;

fn main() {
    let sql_file =
        fs::read_to_string("examples/simple-select.sql").expect("Failed to read SQL file");

    let parse_result = SqlParser::parse(Rule::select, &sql_file);

    match parse_result {
        Ok(ast) => {
            println!("Parsed successfully!");
            println!("{:?}", ast);
        }
        Err(e) => {
            eprintln!("Parse error: {}", e);
        }
    }
}
