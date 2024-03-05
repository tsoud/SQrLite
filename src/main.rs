use std::env;
use std::fs::File;
use std::io::prelude::*;

use anyhow::{bail, Result};

fn main() -> Result<()> {
    let args = env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100]; // an array of 100 zeroes
            file.read_exact(&mut header)?; // read exactly enough bytes to fill `header`

            #[allow(unused_variables)]
            let page_size = u16::from_be_bytes([header[16], header[17]]);

            println!("database page size: {}", page_size);
        }

        _ => bail!("Missing or invalid command: {}", command),
    }

    Ok(())
}
