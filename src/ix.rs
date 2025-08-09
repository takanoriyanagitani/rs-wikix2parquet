use std::io::{self, BufRead};
use std::str::FromStr;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexEntry {
    /// The byte offset. e.g., 569
    pub byte_offset: u64,

    /// The article id(may be always integer). e.g., 10
    pub article_id: String,

    /// The title which can contain the separator. e.g., "The Movie 3: Sub"
    pub title: String,
}

impl IndexEntry {
    pub fn schema() -> Schema {
        Schema::new(vec![
            Field::new("byte_offset", DataType::UInt64, false),
            Field::new("article_id", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, false),
        ])
    }
}

impl FromStr for IndexEntry {
    type Err = io::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut splited = s.splitn(3, ':');

        // Byte offset
        let byte_offset_str = splited
            .next()
            .ok_or(io::Error::other("byte offset missing"))?;

        // Article id
        let article_id_str = splited
            .next()
            .ok_or(io::Error::other("article id missing"))?;

        // Title
        let title_str = splited.next().ok_or(io::Error::other("title missing"))?;

        let byte_offset: u64 = str::parse(byte_offset_str).map_err(io::Error::other)?;

        Ok(Self {
            byte_offset,
            article_id: article_id_str.into(),
            title: title_str.into(),
        })
    }
}

pub fn lines2entries<I>(lines: I) -> impl Iterator<Item = Result<IndexEntry, io::Error>>
where
    I: Iterator<Item = Result<String, io::Error>>,
{
    lines.map(|res_line| res_line.and_then(|line| str::parse(line.as_str())))
}

pub fn bufread2entries<R>(brdr: R) -> impl Iterator<Item = Result<IndexEntry, io::Error>>
where
    R: BufRead,
{
    lines2entries(brdr.lines())
}

pub fn stdin2entries() -> impl Iterator<Item = Result<IndexEntry, io::Error>> {
    bufread2entries(io::stdin().lock())
}
