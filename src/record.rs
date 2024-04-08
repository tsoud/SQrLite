use std::fmt;
use std::{cmp::min, error::Error};

use crate::cell::CellContent;
use crate::varint::{decode_be, MaxBytesExceededError};

#[derive(Debug)]
pub struct ParseError {
    details: String,
}

impl ParseError {
    fn new(data_type: &str) -> Self {
        Self {
            details: format!("Failed to parse field as `{}`", data_type),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.details)
    }
}

impl Error for ParseError {}

#[derive(Debug)]
pub enum DataType {
    Null,
    BooleanFalse,
    BooleanTrue,
    Integer,
    Real,
    Text,
    Blob,
}

#[derive(Debug)]
pub enum FieldData {
    Null(()),
    BooleanFalse(u8),
    BooleanTrue(u8),
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl FieldData {
    fn parse(data_type: DataType, data: &[u8]) -> Result<Self, ParseError> {
        match data_type {
            DataType::Null => {
                if !data.is_empty() {
                    return Err(ParseError::new("NULL"));
                }
                Ok(FieldData::Null(()))
            }
            DataType::BooleanFalse => {
                if !data.is_empty() {
                    return Err(ParseError::new("FALSE"));
                }
                Ok(FieldData::BooleanFalse(0))
            }
            DataType::BooleanTrue => {
                if !data.is_empty() {
                    return Err(ParseError::new("True"));
                }
                Ok(FieldData::BooleanTrue(1))
            }
            DataType::Integer => {
                let value = match data.len() {
                    1 => i8::from_be_bytes([data[0]]) as i64,
                    2 => i16::from_be_bytes([data[0], data[1]]) as i64,
                    3 => {
                        // Adjust for 24-bit data
                        if data[0] <= 0x7F {
                            i32::from_be_bytes([0, data[0], data[1], data[2]]) as i64
                        } else {
                            i32::from_be_bytes([0xFF, data[0], data[1], data[2]]) as i64
                        }
                    }
                    4 => i32::from_be_bytes([data[0], data[1], data[2], data[3]]) as i64,
                    6 => {
                        // Adjust for 48-bit data
                        if data[0] <= 0x7F {
                            i64::from_be_bytes([
                                0, 0, data[0], data[1], data[2], data[3], data[4], data[5],
                            ])
                        } else {
                            i64::from_be_bytes([
                                0xFF, 0xFF, data[0], data[1], data[2], data[3], data[4], data[5],
                            ])
                        }
                    }
                    8 => i64::from_be_bytes([
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ]),
                    _ => {
                        return Err(ParseError::new("INTEGER"));
                    }
                };
                Ok(FieldData::Integer(value))
            }
            DataType::Real => {
                if data.len() != 8 {
                    return Err(ParseError::new("REAL"));
                }
                let value = f64::from_be_bytes([
                    data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                ]);
                Ok(FieldData::Real(value))
            }
            DataType::Text => {
                if let Ok(text) = String::from_utf8(data.to_vec()) {
                    Ok(FieldData::Text(text))
                } else {
                    Err(ParseError::new("TEXT"))
                }
            }
            DataType::Blob => Ok(FieldData::Blob(data.into())),
        }
    }
}

#[derive(Debug)]
pub struct Field {
    size: usize,
    offset: usize,
    data_type: DataType,
}

impl Default for Field {
    fn default() -> Self {
        Self {
            size: 0,
            offset: 0,
            data_type: DataType::Null,
        }
    }
}

impl Field {
    pub fn read_data(&self, content: &CellContent) -> Result<FieldData, Box<dyn Error>> {
        let payload = content.get_payload()?;
        let data = &payload[self.offset..self.offset + self.size];

        match self.data_type {
            DataType::Null => {
                let field_value =
                    FieldData::parse(DataType::Null, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
            DataType::BooleanFalse => {
                let field_value =
                    FieldData::parse(DataType::BooleanFalse, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
            DataType::BooleanTrue => {
                let field_value =
                    FieldData::parse(DataType::BooleanTrue, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
            DataType::Integer => {
                let field_value =
                    FieldData::parse(DataType::Integer, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
            DataType::Real => {
                let field_value =
                    FieldData::parse(DataType::Real, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
            DataType::Text => {
                let field_value =
                    FieldData::parse(DataType::Text, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
            DataType::Blob => {
                let field_value =
                    FieldData::parse(DataType::Blob, data).map_err(|e| e.to_string())?;
                Ok(field_value)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct Record {
    pub fields: Option<Vec<Field>>,
}

impl Record {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    pub fn load_fields(&mut self, payload: &[u8]) -> Result<(), MaxBytesExceededError> {
        // read first varint from payload to determine size
        let (header_size, mut idx) = decode_be(&payload[..9usize])?;
        let mut fields = vec![];

        let mut serial_type: u64;
        let mut position = idx;
        let mut field_start = header_size as usize;
        while position < header_size as usize {
            // let mut new_field = Field::default();
            let mut new_field = Field {
                offset: field_start,
                ..Default::default()
            };
            // new_field.offset = field_start;
            let end = min(position + 9usize, header_size as usize);
            (serial_type, idx) = decode_be(&payload[position..end])?;
            match serial_type {
                0 => {
                    new_field.size = 0;
                    new_field.data_type = DataType::Null;
                }
                1..=4 => {
                    new_field.size = serial_type as usize;
                    new_field.data_type = DataType::Integer;
                }
                s @ 5 | s @ 6 => {
                    new_field.size = if s == 5 { 6 } else { 8 };
                    new_field.data_type = DataType::Integer;
                }
                7 => {
                    new_field.size = 8;
                    new_field.data_type = DataType::Real;
                }
                8 => {
                    new_field.size = 0;
                    new_field.data_type = DataType::BooleanFalse;
                }
                9 => {
                    new_field.size = 0;
                    new_field.data_type = DataType::BooleanTrue;
                }
                10 | 11 => {
                    // Technically, 10 and 11 have variable sizes but are reserved for
                    // internal SQLite use and should never appear in database files.
                    todo!()
                }
                _ => {
                    if serial_type % 2 == 0 {
                        new_field.size = ((serial_type - 12) / 2) as usize;
                        new_field.data_type = DataType::Blob;
                    } else {
                        new_field.size = ((serial_type - 13) / 2) as usize;
                        new_field.data_type = DataType::Text
                    }
                }
            };
            field_start += new_field.size;
            fields.push(new_field);
            position += idx;
        }

        self.fields = Some(fields);

        Ok(())
    }
}
