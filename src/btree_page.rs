#![allow(dead_code)]

use std::io::{prelude::*, SeekFrom};
use std::{error::Error, fmt};

use crate::cell::Cell;
use crate::db::Database;

const LEAF_BTREE_HEADER_SIZE: u8 = 8;
const INTERIOR_BTREE_HEADER_SIZE: u8 = 12;

#[derive(Debug)]
struct BtreeTypeError {
    details: String,
}

impl BtreeTypeError {
    fn new() -> Self {
        Self {
            details: "invalid b-tree type".to_owned(),
        }
    }
}

impl fmt::Display for BtreeTypeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for BtreeTypeError {}

#[derive(Debug)]
struct PagesExceededError {
    details: String,
}

impl PagesExceededError {
    fn new() -> Self {
        Self {
            details: "number of pages in database exceeded".to_string(),
        }
    }
}

impl fmt::Display for PagesExceededError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for PagesExceededError {}

#[derive(Debug)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl PageType {
    fn get_page_type(flag: u8) -> Result<Self, BtreeTypeError> {
        match flag {
            0x02 => Ok(Self::InteriorIndex),
            0x05 => Ok(Self::InteriorTable),
            0x0a => Ok(Self::LeafIndex),
            0x0d => Ok(Self::LeafTable),
            _ => Err(BtreeTypeError::new()),
        }
    }

    fn get_header_size(&self) -> u8 {
        match &self {
            PageType::InteriorIndex | PageType::InteriorTable => INTERIOR_BTREE_HEADER_SIZE,
            PageType::LeafIndex | PageType::LeafTable => LEAF_BTREE_HEADER_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct BtreePage {
    pub page_type: PageType,
    pub page_num: u32, // page numbers are indexed from ONE per SQLite convention
    pub file_starting_position: u64, // start of the page relative to beginning of db file in bytes
    pub num_cells: u16,
    pub first_cell_start: u16,
    pub cell_pointers: Vec<u16>,
    pub header_size: u8,
    pub header: [u8; 8],
    pub rightmost_ptr: Option<u32>,
    page_size: u16, // for calculating cell sizes (from db)
}

impl Default for BtreePage {
    fn default() -> Self {
        BtreePage {
            page_type: PageType::LeafTable,
            page_num: 0,
            file_starting_position: 0,
            num_cells: 0,
            first_cell_start: 0,
            cell_pointers: vec![],
            header_size: 8,
            header: [0u8; 8],
            rightmost_ptr: None,
            page_size: 0,
        }
    }
}

impl BtreePage {
    pub fn new(db: &mut Database) -> Result<Self, Box<dyn Error>> {
        let mut btree_pg = BtreePage::default();
        btree_pg
            .read_page_header(db, 1)
            .map_err(|e| e.to_string())?;
        btree_pg.page_size = db.page_size;
        Ok(btree_pg)
    }

    pub fn read_page_header(&mut self, db: &mut Database, page: u32) -> Result<(), Box<dyn Error>> {
        validate_page_num(db, page).map_err(|e| e.to_string())?;
        self.page_num = page;
        self.file_starting_position = ((page - 1) as u64) * (db.page_size as u64);

        self.header = [0u8; 8];
        let pg_header_start: u64 = if page == 1 {
            100
        } else {
            self.file_starting_position
        };

        db.file
            .seek(SeekFrom::Start(pg_header_start))
            .map_err(|e| e.to_string())?;
        db.file
            .read_exact(&mut self.header)
            .map_err(|e| "error reading page header: ".to_owned() + &e.to_string())?;
        // db.file.read_exact_at(&mut page_header, pg_header_start);

        // read btree page type from first byte and get header size
        self.page_type = PageType::get_page_type(self.header[0]).map_err(|e| e.to_string())?;
        self.header_size = self.page_type.get_header_size();
        self.num_cells = u16::from_be_bytes([self.header[3], self.header[4]]);
        self.first_cell_start = u16::from_be_bytes([self.header[5], self.header[6]]);

        // read the right-most pointer if the page is an interior b-tree
        self.rightmost_ptr = match self.page_type {
            PageType::InteriorTable | PageType::InteriorIndex => {
                let mut pointer_buf = [0u8; 4];
                db.file
                    .seek(SeekFrom::Start(
                        pg_header_start + u64::from(self.header_size) - 4,
                    ))
                    .map_err(|e| e.to_string())?;
                db.file
                    .read_exact(&mut pointer_buf)
                    .map_err(|e| e.to_string())?;
                Some(u32::from_be_bytes(pointer_buf))
            }
            _ => None,
        };

        // read the cell pointer array immediately following the page header
        self.cell_pointers = vec![];
        let mut cell_ptr = [0u8; 2];
        for i in (0..self.num_cells * 2).step_by(2) {
            db.file
                .seek(SeekFrom::Start(
                    pg_header_start + u64::from(self.header_size) + u64::from(i),
                ))
                .map_err(|e| e.to_string())?;
            db.file
                .read_exact(&mut cell_ptr)
                .map_err(|e| e.to_string())?;
            self.cell_pointers.push(u16::from_be_bytes(cell_ptr))
        }

        Ok(())
    }

    pub fn get_page_cells(&self) -> Vec<Cell> {
        let mut pointers = self.cell_pointers.clone();
        pointers.sort_unstable();

        pointers
            .iter()
            .enumerate()
            .map(|(i, offset)| {
                let size = if i == pointers.len() - 1 {
                    self.page_size - offset
                } else {
                    pointers[i + 1] - offset
                };
                Cell {
                    offset: *offset as u64,
                    size: size as usize,
                }
            })
            .collect::<Vec<Cell>>()
    }
}

fn validate_page_num(db: &Database, page: u32) -> Result<(), PagesExceededError> {
    if page > db.page_count {
        Err(PagesExceededError::new())
    } else {
        Ok(())
    }
}
