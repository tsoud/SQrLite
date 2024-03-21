use std::env;
use std::error::Error;
use std::fmt;

use rusqlite::db::Database;

#[derive(Debug)]
enum CMDError {
    DBPathNotGiven,
    NoCommandGiven,
    InvalidCommand(String),
}

impl fmt::Display for CMDError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            CMDError::DBPathNotGiven => write!(f, "Missing <database path>"),
            CMDError::NoCommandGiven => write!(f, "Missing <command>"),
            CMDError::InvalidCommand(cmd) => write!(f, "Missing or invalid command: <{}>", cmd),
        }
    }
}

impl Error for CMDError {}

fn main() -> Result<(), Box<dyn Error>> {
    let args = env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => {
            eprintln!("{}", CMDError::DBPathNotGiven);
            std::process::exit(1)
        }
        2 => {
            eprintln!("{}", CMDError::NoCommandGiven);
            std::process::exit(1)
        }
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let db = Database::new(&args[1])?;
            // let db_info = DBInfo::read_info(&db)?;

            println!(
                "{:24}{:<1}\n{:24}{:<1}",
                "database page size:", db.page_size, "database page count:", db.page_count
            );
        }
        _ => {
            eprintln!("{}", CMDError::InvalidCommand(command.clone()));
            std::process::exit(1)
        }
    }

    Ok(())
}
