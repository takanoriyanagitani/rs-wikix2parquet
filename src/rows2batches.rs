use std::io;
use std::sync::Arc;

use arrow::array::UInt64Builder;
use arrow::array::{StringArray, UInt64Array};

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use crate::ix::IndexEntry;

pub struct IndexBatchIter<I> {
    pub inner: I,
    pub schema: Arc<Schema>,
    pub batch_size: usize,
}

impl<I> Iterator for IndexBatchIter<I>
where
    I: Iterator<Item = Result<IndexEntry, io::Error>>,
{
    type Item = Result<RecordBatch, io::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let sz = self.batch_size;

        let mut ofb: UInt64Builder = UInt64Array::builder(sz);

        let mut ids: Vec<String> = Vec::with_capacity(sz);
        let mut strings: Vec<String> = Vec::with_capacity(sz);

        let taken = self.inner.by_ref().take(sz);

        for res in taken {
            match res {
                Ok(ie) => {
                    ofb.append_value(ie.byte_offset);
                    ids.push(ie.article_id);
                    strings.push(ie.title);
                }
                Err(e) => return Some(Err(e)),
            }
        }

        let offsets: UInt64Array = ofb.finish();
        let aids: StringArray = ids.into();
        let titles: StringArray = strings.into();

        if offsets.is_empty() {
            return None;
        }

        let batch: Result<_, _> = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(offsets), Arc::new(aids), Arc::new(titles)],
        )
        .map_err(io::Error::other);

        Some(batch)
    }
}
