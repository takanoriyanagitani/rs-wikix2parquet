use std::io::{self, Write};
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

pub struct EntriesToParquet<W>
where
    W: Write,
{
    pub writer: ArrowWriter<W>,
}

impl<W> EntriesToParquet<W>
where
    W: Write + Send,
{
    pub fn from_wtr(w: W, c: Compression, s: Arc<Schema>) -> Result<Self, io::Error> {
        let props = WriterProperties::builder().set_compression(c).build();
        let writer = ArrowWriter::try_new(w, s, Some(props))?;
        Ok(Self { writer })
    }
}

impl<W> EntriesToParquet<W>
where
    W: Write + Send,
{
    pub fn write_batch(&mut self, b: &RecordBatch) -> Result<(), io::Error> {
        self.writer.write(b).map_err(io::Error::other)
    }
}

impl<W> EntriesToParquet<W>
where
    W: Write + Send,
{
    pub fn write_all<I>(&mut self, batches: I) -> Result<(), io::Error>
    where
        I: Iterator<Item = Result<RecordBatch, io::Error>>,
    {
        for batch_result in batches {
            let batch = batch_result?; // propagate iterator errors
            self.write_batch(&batch)?; // propagate write errors
        }
        self.writer.flush().map_err(io::Error::other)
    }
}

pub fn iter2wtr<I, W>(i: I, w: W, c: Compression, s: Arc<Schema>) -> Result<(), io::Error>
where
    I: Iterator<Item = Result<RecordBatch, io::Error>>,
    W: Write + Send,
{
    // Create the writer
    let mut etp = EntriesToParquet::from_wtr(w, c, s)?;

    // Write every batch that the iterator yields
    etp.write_all(i)?;

    // Close the ArrowWriter so that the footer gets written
    etp.writer.close().map(|_| ()).map_err(io::Error::other)
}
