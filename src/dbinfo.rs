#![allow(dead_code)]

use std::env::current_dir;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;

// use anyhow::Error;

const DB_HEADER_SIZE: usize = 100;
const BTREE_HEADER_SIZE: usize = 8;
const INTERIOR_BTREE_HEADER_SIZE: usize = BTREE_HEADER_SIZE + 4;
// (offset, number of bytes) for page information in the db header:
const PG_SIZE: (usize, usize) = (16, 2);
const PG_COUNT: (usize, usize) = (28, 4);

#[derive(Debug)]
pub struct DBInfo {
    pub db_page_size: u16,
    pub db_page_count: u32,
    pub num_tables: u32,
    pub num_indexes: u32,
    pub num_triggers: u32,
    pub num_views: u32,
}

impl Default for DBInfo {
    fn default() -> Self {
        DBInfo {
            db_page_size: 512, // minimum page size allowed by SQLite
            db_page_count: 1,
            num_tables: 0,
            num_indexes: 0,
            num_triggers: 0,
            num_views: 0,
        }
    }
}

pub fn parse_db_header<P>(db_file: P) -> Result<(u16, u32), Box<dyn Error>>
where
    P: AsRef<Path>,
{
    let mut path = db_file.as_ref().to_path_buf();
    if !path.is_absolute() {
        let cwd = current_dir()?;
        path = cwd.join(path);
    }

    let mut file = File::open(path)?;
    let mut header = [0; DB_HEADER_SIZE];
    file.read_exact(&mut header)?;

    let pg_size_arr = header[(PG_SIZE.0)..(PG_SIZE.0 + PG_SIZE.1)].try_into()?;
    let page_size = u16::from_be_bytes(pg_size_arr);

    let pg_count_arr = header[(PG_COUNT.0)..(PG_COUNT.0 + PG_COUNT.1)].try_into()?;
    let page_count = u32::from_be_bytes(pg_count_arr);

    Ok((page_size, page_count))
}

impl DBInfo {
    pub fn new<P>(db_file: P) -> Result<Self, Box<dyn Error>>
    where
        P: AsRef<Path>,
    {
        let (page_size, page_count) = parse_db_header(db_file)?;
        Ok(Self {
            db_page_size: page_size,
            db_page_count: page_count,
            ..Default::default()
        })
    }

    fn read_schema_info() {
        todo!();
    }

    fn parse_page_header() {
        // input: page_number
        todo!();
    }
}
