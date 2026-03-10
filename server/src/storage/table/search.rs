use std::sync::{Arc, Mutex};
use super::super::row::Row;
use super::super::search::SearchIndex;
use super::Table;

impl Table {
    pub fn setup_search_index(&mut self, path: &str) -> anyhow::Result<()> {
        let text_fields: Vec<String> = self
            .columns
            .iter()
            .filter(|c| c.data_type == crate::storage::DataType::Text || c.data_type == crate::storage::DataType::VarChar)
            .map(|c| c.name.clone())
            .collect();

        if !text_fields.is_empty() {
            let index = SearchIndex::new(path, &text_fields)?;
            self.search_index = Some(Arc::new(Mutex::new(index)));

            // Populate existing data
            let rows_to_index = self.rows.clone();
            for row in rows_to_index {
                self.index_row(&row)?;
            }
        }
        Ok(())
    }

    pub(crate) fn index_row(&self, row: &Row) -> anyhow::Result<()> {
        if let Some(ref search_index) = self.search_index {
            let mut field_values = Vec::new();
            for (i, col) in self.columns.iter().enumerate() {
                if (col.data_type == crate::storage::DataType::Text || col.data_type == crate::storage::DataType::VarChar)
                    && let Some(val) = row.values.get(i).and_then(|v| v.as_text())
                {
                    field_values.push((col.name.clone(), val.to_string()));
                }
            }
            search_index
                .lock()
                .unwrap()
                .add_document(&row.id, &field_values)?;
        }
        Ok(())
    }
}
