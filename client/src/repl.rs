use anyhow::Result;
use rustyline::{Editor, history::DefaultHistory};
use crate::http;

pub async fn start(host: String, port: u16) -> Result<()> {
    let mut rl: Editor<(), DefaultHistory> = Editor::new()?;
    
    // Check if we can find a history file
    let history_path = "history.txt";
    if rl.load_history(history_path).is_err() {
        // Ignore error if history file doesn't exist
    }

    println!("thy-squeal client v{}", env!("CARGO_PKG_VERSION"));
    println!("Connected to http://{}:{}", host, port);
    println!("Type .help for commands, .quit to exit\n");

    loop {
        let readline = rl.readline("thy> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                rl.add_history_entry(line)?;

                if line == ".quit" || line == ".exit" {
                    break;
                }

                if line == ".help" {
                    print_help();
                    continue;
                }

                if line.starts_with('.') {
                    println!("Unknown command: {}", line);
                    print_help();
                    continue;
                }

                // Execute SQL via HTTP
                if let Err(e) = http::execute_query(&host, port, line).await {
                    eprintln!("Error: {}", e);
                }
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                println!("Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history(history_path)?;
    Ok(())
}

fn print_help() {
    println!("Available commands:");
    println!("  .help     - Show this help");
    println!("  .quit     - Exit the REPL");
    println!("  .exit     - Exit the REPL");
    println!();
    println!("SQL queries can be entered directly.");
}
