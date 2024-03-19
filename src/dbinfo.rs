#![allow(dead_code)]

use std::error::Error;

use crate::db::Database;

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

impl DBInfo {
    pub fn read_info(db: &Database) -> Result<Self, Box<dyn Error>> {
        let pg_size_arr = db.header[(PG_SIZE.0)..(PG_SIZE.0 + PG_SIZE.1)]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| {
                "error reading header: ".to_owned() + &e.to_string()
            })?;
        let page_size = u16::from_be_bytes(pg_size_arr);

        let pg_count_arr = db.header[(PG_COUNT.0)..(PG_COUNT.0 + PG_COUNT.1)]
            .try_into()
            .map_err(|e: std::array::TryFromSliceError| {
                "error reading header: ".to_owned() + &e.to_string()
            })?;
        let page_count = u32::from_be_bytes(pg_count_arr);

        Ok(Self {
            db_page_size: page_size,
            db_page_count: page_count,
            ..Default::default()
        })
    }

    // fn read_schema_info() {
    //     todo!();
    // }

    // fn parse_page_header(&self, pg_number: usize) {
    //     // input: page_number
    //     todo!();
    // }
}
