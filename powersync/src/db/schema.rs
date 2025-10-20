use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Serialize, ser::SerializeStruct};

use crate::error::PowerSyncError;

type SchemaString = Cow<'static, str>;

#[derive(Serialize, Default, Debug)]
pub struct Schema {
    pub tables: Vec<Table>,
    pub raw_tables: Vec<RawTable>,
}

impl Schema {
    /// Validates the schema by ensuring there are no duplicate table names and that each table is
    /// valid.
    pub fn validate(&self) -> Result<(), PowerSyncError> {
        let mut table_names = HashSet::new();
        for table in &self.tables {
            if !table_names.insert(table.name.as_ref()) {
                return Err(PowerSyncError::argument_error(format!(
                    "Duplicate table name: {}",
                    table.name,
                )));
            }

            table.validate()?;
        }

        Ok(())
    }

    fn is_invalid_name_char(c: char) -> bool {
        // Specialized implementation of the regex ["'%,.#\s\[\]]
        matches!(c, '"' | '\'' | '%' | ',' | '.' | '#' | '[' | ']') || c.is_whitespace()
    }

    fn validate_name(name: &str, kind: &'static str) -> Result<(), PowerSyncError> {
        if name.contains(Self::is_invalid_name_char) {
            Err(PowerSyncError::argument_error(format!(
                "Name for {kind} ({name}) contains invalid characters."
            )))
        } else {
            Ok(())
        }
    }
}

/// A PowerSync-managed table.
///
/// When this is part of a schema, the PowerSync SDK will create and auto-migrate the table.
/// If you need direct control on a table, use [RawTable] instead.
#[derive(Debug)]
pub struct Table {
    /// The synced table name, matching sync rules.
    pub name: SchemaString,
    // Override the name for the view.
    pub view_name_override: Option<SchemaString>,
    /// List of columns.
    pub columns: Vec<Column>,
    /// List of indexes.
    pub indexes: Vec<Index>,
    /// Whether this is a local-only table.
    pub local_only: bool,
    /// Whether this is an insert-only table.
    pub insert_only: bool,
    /// Whether to add a hidden `_metadata` column that will be enabled for updates to attach custom
    /// information about writes that will be reported through crud entries.
    pub track_metadata: bool,
    /// When set, track old values of columns for CRUD entries.
    ///
    /// See [TrackPreviousValues] for details.
    pub track_previous_values: Option<TrackPreviousValues>,
    /// Whether an `UPDATE` statement that doesn't change any values should be ignored when creating
    /// CRUD entries.
    pub ignore_empty_updates: bool,
}

impl Table {
    /// Creates a new table from its `name` and `columns`.
    ///
    /// Additional options can be set with the `build` callback.
    pub fn create(
        name: impl Into<SchemaString>,
        columns: Vec<Column>,
        build: impl FnOnce(&mut Table),
    ) -> Self {
        let mut table = Self {
            name: name.into(),
            view_name_override: None,
            columns,
            indexes: vec![],
            local_only: false,
            insert_only: false,
            track_metadata: false,
            track_previous_values: None,
            ignore_empty_updates: false,
        };
        build(&mut table);
        table
    }

    fn validate(&self) -> Result<(), PowerSyncError> {
        if self.columns.len() > Self::MAX_AMOUNT_OF_COLUMNS {
            return Err(PowerSyncError::argument_error(format!(
                "Has more than {} columns, which is not supported",
                Self::MAX_AMOUNT_OF_COLUMNS
            )));
        }

        Schema::validate_name(&self.name, "table")?;
        if let Some(view_name_override) = &self.view_name_override {
            Schema::validate_name(view_name_override, "table view")?;
        }

        if self.local_only && self.track_metadata {
            return Err(PowerSyncError::argument_error(
                "Can't track metadata for local-only tables",
            ));
        }

        if self.local_only && self.track_previous_values.is_some() {
            return Err(PowerSyncError::argument_error(
                "Can't track old values for local-only tables",
            ));
        }

        let mut column_names = HashSet::new();
        column_names.insert("id");
        for column in &self.columns {
            if column.name == "id" {
                return Err(PowerSyncError::argument_error(
                    "id column is automatically added, cusotm id columns are not supported",
                ));
            }

            if !column_names.insert(column.name.as_ref()) {
                return Err(PowerSyncError::argument_error(format!(
                    "Duplicate column: {}",
                    column.name
                )));
            }

            Schema::validate_name(&column.name, "column")?;
        }

        let mut index_names = HashSet::new();
        for index in &self.indexes {
            if !index_names.insert(index.name.as_ref()) {
                return Err(PowerSyncError::argument_error(format!(
                    "Duplicate index: {}",
                    index.name
                )));
            }
            Schema::validate_name(&index.name, "index")?;

            for column in &index.columns {
                if !column_names.contains(column.name.as_ref()) {
                    return Err(PowerSyncError::argument_error(format!(
                        "Column: {} not found for index {}",
                        column.name, index.name,
                    )));
                }
            }
        }

        Ok(())
    }

    const MAX_AMOUNT_OF_COLUMNS: usize = 1999;
}

impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut serializer = serializer.serialize_struct("Table", 10)?;
        serializer.serialize_field("name", &self.name)?;
        serializer.serialize_field("columns", &self.columns)?;
        serializer.serialize_field("indexes", &self.indexes)?;
        serializer.serialize_field("local_only", &self.local_only)?;
        serializer.serialize_field("insert_only", &self.insert_only)?;
        serializer.serialize_field("view_name", &self.view_name_override)?;
        serializer.serialize_field("ignore_empty_update", &self.ignore_empty_updates)?;
        serializer.serialize_field("include_metadata", &self.track_metadata)?;

        if let Some(include_old) = &self.track_previous_values {
            if let Some(filter) = &include_old.column_filter {
                serializer.serialize_field("include_old", &filter)?;
            } else {
                serializer.serialize_field("include_old", &true)?;
            }

            serializer.serialize_field(
                "include_old_only_when_changed",
                &include_old.only_when_changed,
            )?;
        } else {
            serializer.serialize_field("include_old_include_oldonly_when_changed", &false)?;
            serializer.serialize_field("include_old_only_when_changed", &false)?;
        }

        serializer.end()
    }
}

#[derive(Serialize, Debug)]
pub struct Column {
    pub name: SchemaString,
    #[serde(rename = "type")]
    pub column_type: ColumnType,
}

impl Column {
    pub fn text(name: impl Into<SchemaString>) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Text,
        }
    }

    pub fn integer(name: impl Into<SchemaString>) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Integer,
        }
    }

    pub fn real(name: impl Into<SchemaString>) -> Self {
        Self {
            name: name.into(),
            column_type: ColumnType::Real,
        }
    }
}

#[derive(Serialize, Debug)]
pub enum ColumnType {
    #[serde(rename = "INTEGER")]
    Integer,
    #[serde(rename = "TEXT")]
    Text,
    #[serde(rename = "REAL")]
    Real,
}

#[derive(Serialize, Debug)]
pub struct Index {
    pub name: SchemaString,
    pub columns: Vec<IndexedColumn>,
}

#[derive(Serialize, Debug)]
pub struct IndexedColumn {
    pub name: SchemaString,
    pub ascending: bool,
    #[serde(rename = "type")]
    pub type_name: SchemaString,
}

#[derive(Serialize, Debug)]
pub struct RawTable {
    pub name: SchemaString,
    pub put: PendingStatement,
    pub delete: PendingStatement,
}

#[derive(Serialize, Debug)]
pub struct PendingStatement {
    pub sql: SchemaString,
    /// This vec should contain an entry for each parameter in [sql].
    pub params: Vec<PendingStatementValue>,
}

#[derive(Serialize, Debug)]
pub enum PendingStatementValue {
    Id,
    Column(SchemaString),
}

/// Options to include old values in CRUD entries for update statements.
///
/// These operations are enabled py passing them to a non-local [Table] constructor.
#[derive(Serialize, Debug)]
pub struct TrackPreviousValues {
    pub column_filter: Option<Vec<SchemaString>>,
    pub only_when_changed: bool,
}

impl TrackPreviousValues {
    pub fn all() -> Self {
        Self {
            column_filter: None,
            only_when_changed: false,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::schema::{Column, Table, TrackPreviousValues};

    #[test]
    fn handles_options_track_metadata() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.track_metadata = true
        }))
        .unwrap();

        assert!(
            value
                .as_object()
                .unwrap()
                .get("include_metadata")
                .unwrap()
                .as_bool()
                .unwrap(),
        );
    }

    #[test]
    fn handles_options_ignore_empty_updates() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.ignore_empty_updates = true
        }))
        .unwrap();

        assert!(
            value
                .as_object()
                .unwrap()
                .get("ignore_empty_update")
                .unwrap()
                .as_bool()
                .unwrap(),
        );
    }

    #[test]
    fn handles_options_track_previous_all() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.track_previous_values = Some(TrackPreviousValues::all())
        }))
        .unwrap();
        let value = value.as_object().unwrap();

        assert!(value.get("include_old").unwrap().as_bool().unwrap());
        assert!(
            !value
                .get("include_old_only_when_changed")
                .unwrap()
                .as_bool()
                .unwrap(),
        );
    }

    #[test]
    fn handles_options_track_previous_column_filter() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.track_previous_values = Some(TrackPreviousValues::all())
        }))
        .unwrap();
        let value = value.as_object().unwrap();

        assert!(value.get("include_old").unwrap().as_bool().unwrap());
        assert!(
            !value
                .get("include_old_only_when_changed")
                .unwrap()
                .as_bool()
                .unwrap(),
        );
    }

    #[test]
    fn invalid_table_name() {
        let mut table = Table::create("#invalid-table", vec![], |tbl| {});
        assert!(table.validate().is_err());

        table.name = "valid".into();
        assert!(table.validate().is_ok());
    }

    #[test]
    fn invalid_duplicate_columns() {
        let mut table = Table::create("tbl", vec![], |tbl| tbl.columns.push(Column::text("a")));
        assert!(table.validate().is_ok());

        table.columns.push(Column::integer("a"));
        assert!(table.validate().is_err());
    }
}
