use std::borrow::Cow;
use std::collections::HashSet;

use serde::{Serialize, Serializer, ser::SerializeStruct};

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

        for table in &self.raw_tables {
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
#[derive(Debug, Serialize)]
pub struct Table {
    /// The synced table name, matching sync rules.
    pub name: SchemaString,
    // Override the name for the view.
    #[serde(rename = "view_name")]
    pub view_name_override: Option<SchemaString>,
    /// List of columns.
    pub columns: Vec<Column>,
    /// List of indexes.
    pub indexes: Vec<Index>,
    #[serde(flatten)]
    pub options: TableOptions,
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
            options: TableOptions::default(),
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

        self.options.validate()?;

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

/// Options that apply to both view-based JSON tables and raw tables.
#[derive(Debug, Default)]
pub struct TableOptions {
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

impl TableOptions {
    fn validate(&self) -> Result<(), PowerSyncError> {
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

        Ok(())
    }
}

impl Serialize for TableOptions {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut serializer = serializer.serialize_struct(
            "TableOptions",
            4 + if self.track_previous_values.is_some() {
                2
            } else {
                0
            },
        )?;

        serializer.serialize_field("local_only", &self.local_only)?;
        serializer.serialize_field("insert_only", &self.insert_only)?;
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
            serializer.skip_field("include_old")?;
            serializer.skip_field("include_old_only_when_changed")?;
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

/// A raw table, defined by the user instead of being managed by PowerSync.
///
/// Any ordinary SQLite table can be defined as a raw table, which enables:
///
///  - More performant queries, since data is stored in typed rows instead of the schemaless JSON
///    view PowerSync uses by default.
///  - More control over the table, since custom column constraints can be used in its definition.
///
///  By default, the PowerSync client will infer the schema of raw tables and use that to generate
/// `UPSERT` and `DELETE` statements to forward writes from the backend database to SQLite. This
/// requires [Self::schema] to be set.
/// These statements can be customized by providing [Self::put] and [Self::delete] statements.
///
/// When using raw tables, you are responsible for creating and migrating them when they've changed.
/// Further, triggers are necessary to collect local writes to those tables. For more information,
/// see [the documentation](https://docs.powersync.com/client-sdks/advanced/raw-tables).
#[derive(Serialize, Debug)]
pub struct RawTable {
    /// The name of the table as used by the sync service.
    ///
    /// This doesn't necessarily have to match the name of the SQLite table that [put] and [delete]
    /// write to. Instead, it's used by the sync client to identify which statements to use when it
    /// encounters sync operations for this table.
    pub name: SchemaString,

    /// An optional schema containing the name of the raw table in the local schema.
    ///
    /// If this is set, [Self::put] and [Self::delete] can be omitted because these statements can
    /// be inferred from the schema.
    #[serde(flatten)]
    pub schema: Option<RawTableSchema>,

    /// A statement responsible for inserting or updating a row in this raw table based on data from
    /// the sync service.
    ///
    /// By default, the client generates an `INSERT` statement with an upsert clause for all columns
    /// in the table.
    ///
    /// See [PendingStatement] for details.
    pub put: Option<PendingStatement>,

    /// A statement responsible for deleting a row based on its PowerSync id.
    ///
    /// By default, the client generates the statement `DELETE FROM $local_table WHERE id = ?`.
    ///
    /// See [PendingStatement] for details. Note that [PendingStatementValue]s used here must all be
    /// [PendingStatementValue::Id].
    pub delete: Option<PendingStatement>,

    /// An optional statement to run when the `powersync_clear` SQL function is called.
    pub clear: Option<SchemaString>,
}

impl RawTable {
    /// Creates a [RawTable] where statements used to sync rows into the table are inferred from
    /// the columns of the table.
    pub fn with_schema(name: impl Into<SchemaString>, schema: RawTableSchema) -> Self {
        Self {
            name: name.into(),
            schema: Some(schema),
            put: None,
            delete: None,
            clear: None,
        }
    }

    /// Creates a [RawTable] with explicit put and delete statements to use.
    pub fn with_statements(
        name: impl Into<SchemaString>,
        put: PendingStatement,
        delete: PendingStatement,
    ) -> Self {
        Self {
            name: name.into(),
            schema: None,
            put: Some(put),
            delete: Some(delete),
            clear: None,
        }
    }

    fn validate(&self) -> Result<(), PowerSyncError> {
        if let Some(schema) = &self.schema {
            schema.options.validate()?;
        } else {
            // If we don't have a schema, statements need to be given.
            if self.put.is_none() || self.delete.is_none() {
                return Err(PowerSyncError::argument_error(
                    "Raw tables without a schema need to provide put and delete statements.",
                ));
            }
        }

        Ok(())
    }
}

/// Information about the schema of a [RawTable] in the local database.
///
/// This information is optional when declaring raw tables. However, providing it allows the sync
/// client to infer [RawTable::put] and [RawTable::delete] statements automatically.
#[derive(Serialize, Debug)]
pub struct RawTableSchema {
    /// The actual name of the raw table in the local schema.
    ///
    /// This is used to infer statements for the sync client. It can also be used to auto-generate
    /// triggers forwarding writes on raw tables into the CRUD upload queue.
    pub table_name: SchemaString,
    /// An optional filter of columns that should be synced.
    ///
    /// By default, all columns in a raw table are considered to be synced. If a filter is
    /// specified, PowerSync treats unmatched columns as _local-only_ and will not attempt to sync
    /// them.
    pub synced_columns: Option<Vec<SchemaString>>,

    /// Common options affecting how the `powersync_create_raw_table_crud_trigger` SQL function
    /// generates triggers.
    #[serde(flatten)]
    pub options: TableOptions,
}

impl RawTableSchema {
    pub fn new(table_name: impl Into<SchemaString>) -> Self {
        Self {
            table_name: table_name.into(),
            synced_columns: None,
            options: Default::default(),
        }
    }
}

/// An SQL statement to be run by the sync client against raw tables.
///
/// Since raw tables are managed by the user, PowerSync can't know how to apply serverside changes
/// to them. These statements bridge raw tables and PowerSync by providing upserts and delete
/// statements.
///
/// For more information, see [the documentation](https://docs.powersync.com/client-sdks/advanced/raw-tables).
#[derive(Serialize, Debug)]
pub struct PendingStatement {
    pub sql: SchemaString,
    /// This vec should contain an entry for each parameter in [sql].
    pub params: Vec<PendingStatementValue>,
}

/// A description of a value that will be resolved in the sync client when running a
/// [PendingStatement] for a [RawTable].
#[derive(Serialize, Debug)]
pub enum PendingStatementValue {
    /// A value that is bound to the textual id used in the PowerSync protocol.
    Id,

    /// A value that is bound to the value of a column in a replace (`PUT`)
    /// operation of the PowerSync protocol.
    Column(SchemaString),

    /// A value that is bound to a JSON object containing all columns from the synced row that
    /// haven't been matched by a [Self::Column] value.
    Rest,
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
    use crate::schema::{Column, RawTable, RawTableSchema, Table, TrackPreviousValues};
    use serde_json::json;

    #[test]
    fn handles_options_track_metadata() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.options.track_metadata = true
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
            tbl.options.ignore_empty_updates = true
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
            tbl.options.track_previous_values = Some(TrackPreviousValues::all())
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
            tbl.options.track_previous_values = Some(TrackPreviousValues {
                column_filter: Some(vec!["a".into()]),
                only_when_changed: true,
            })
        }))
        .unwrap();
        let value = value.as_object().unwrap();

        let include_old = value.get("include_old").unwrap();
        assert_eq!(*include_old, json!(["a"]));
        assert_eq!(
            *value.get("include_old_only_when_changed").unwrap(),
            json!(true)
        );
    }

    #[test]
    fn invalid_table_name() {
        let mut table = Table::create("#invalid-table", vec![], |_| {});
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

    #[test]
    fn invalid_raw_table_missing_statements() {
        let mut table = RawTable::with_schema("users", RawTableSchema::new("users"));
        table.schema = None;

        assert!(table.validate().is_err());
    }
}
