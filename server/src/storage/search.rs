use crate::storage::Column;
use anyhow::Result;
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Value as TantivyValue;
use tantivy::schema::*;
use tantivy::{Index, IndexWriter, ReloadPolicy};

pub struct SearchIndex {
    index: Index,
    writer: IndexWriter,
    schema: Schema,
}

impl SearchIndex {
    pub fn new(path: &str, fields: &[String]) -> Result<Self> {
        let mut schema_builder = Schema::builder();
        schema_builder.add_text_field("row_id", STRING | STORED);

        for field in fields {
            schema_builder.add_text_field(field, TEXT | STORED);
        }

        let schema = schema_builder.build();
        let index = if std::path::Path::new(path).exists() {
            Index::open_in_dir(path)?
        } else {
            std::fs::create_dir_all(path)?;
            Index::create_in_dir(path, schema.clone())?
        };

        // 50MB heap for indexer
        let writer = index.writer(50_000_000)?;

        Ok(Self {
            index,
            writer,
            schema,
        })
    }

    pub fn index_document(
        &mut self,
        row_id: &str,
        columns: &[Column],
        values: &[crate::storage::Value],
    ) -> Result<()> {
        let mut doc = tantivy::TantivyDocument::default();
        let row_id_field = self.schema.get_field("row_id")?;
        doc.add_text(row_id_field, row_id);

        for (i, col) in columns.iter().enumerate() {
            if let Ok(field) = self.schema.get_field(&col.name) {
                doc.add_text(field, values[i].to_string());
            }
        }

        self.writer.add_document(doc)?;
        self.writer.commit()?;
        Ok(())
    }

    pub fn delete_document(&mut self, row_id: &str) -> Result<()> {
        let row_id_field = self.schema.get_field("row_id")?;
        let term = tantivy::Term::from_field_text(row_id_field, row_id);
        self.writer.delete_term(term);
        self.writer.commit()?;
        Ok(())
    }

    pub fn search(&self, query_str: &str, limit: usize) -> Result<Vec<(String, f32)>> {
        let reader = self
            .index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;

        let searcher = reader.searcher();
        let mut text_fields = Vec::new();
        for (field, entry) in self.schema.fields() {
            if field != self.schema.get_field("row_id")?
                && let FieldType::Str(text_options) = entry.field_type()
                && text_options.get_indexing_options().is_some()
            {
                text_fields.push(field);
            }
        }

        let query_parser = QueryParser::for_index(&self.index, text_fields);
        let query = query_parser.parse_query(query_str)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;

        let mut results = Vec::new();
        let row_id_field = self.schema.get_field("row_id")?;

        for (score, doc_address) in top_docs {
            let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)?;
            if let Some(val) = retrieved_doc.get_first(row_id_field)
                && let Some(id_str) = val.as_str()
            {
                results.push((id_str.to_string(), score));
            }
        }

        Ok(results)
    }
}
