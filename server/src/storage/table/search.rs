use super::super::row::Row;
use super::super::search::SearchIndex;
use super::Table;
use std::sync::{Arc, Mutex};

impl Table {
    pub fn enable_search_index(&mut self, data_dir: &str) {
        let path = format!("{}/search_{}", data_dir, self.schema.name);
        let fields: Vec<String> = self.schema.columns.iter().map(|c| c.name.clone()).collect();
        let mut index = SearchIndex::new(&path, &fields).unwrap();

        // Index existing rows
        for row in &self.data.rows {
            index
                .index_document(&row.id, &self.schema.columns, &row.values)
                .unwrap();
        }

        self.indexes.search = Some(Arc::new(Mutex::new(index)));
    }

    pub fn index_row(&self, row: &Row) -> Result<(), String> {
        if let Some(ref index) = self.indexes.search {
            index
                .lock()
                .unwrap()
                .index_document(&row.id, &self.schema.columns, &row.values)
                .map_err(|e| e.to_string())
        } else {
            Ok(())
        }
    }
}
