use std::collections::HashSet;

use serde::{Serialize, ser::SerializeStruct};

use crate::error::PowerSyncError;

#[derive(Serialize, Default, Debug)]
pub struct Schema<'a> {
    pub tables: Vec<Table<'a>>,
    pub raw_tables: Vec<RawTable<'a>>,
}

/// A PowerSync-managed table.
///
/// When this is part of a schema, the PowerSync SDK will create and auto-migrate the table.
/// If you need direct control on a table, use [RawTable] instead.
#[derive(Debug)]
pub struct Table<'a> {
    /// The synced table name, matching sync rules.
    pub name: &'a str,
    // Override the name for the view.
    pub view_name_override: Option<&'a str>,
    /// List of columns.
    pub columns: Vec<Column<'a>>,
    /// List of indexes.
    pub indexes: Vec<Index<'a>>,
    /// WHether this is a local-only table.
    pub local_only: bool,
    /// Whether this is an insert-only table.
    pub insert_only: bool,
    /// Whether to add a hiden `_metadata` column that will be enabled for updates to attach custom
    /// information about writes that will be reported through crud entries.
    pub track_metadata: bool,
    /// When set, track old values of columns for CRUD entries.
    ///
    /// See [TrackPreviousValues] for details.
    pub track_previous_values: Option<TrackPreviousValues<'a>>,
    /// Whether an `UPDATE` statement that doesn't change any values should be ignored when creating
    /// CRUD entries.
    pub ignore_empty_updates: bool,
}

impl<'a> Table<'a> {
    /// Creates a new table from its `name` and `columns`.
    ///
    /// Additional options can be set with the `build` callback.
    pub fn create(
        name: &'a str,
        columns: Vec<Column<'a>>,
        build: impl FnOnce(&mut Table<'a>) -> (),
    ) -> Self {
        let mut table = Self {
            name,
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

    pub fn validate(&self) -> Result<(), PowerSyncError> {
        if self.columns.len() > Self::MAX_AMOUNT_OF_COLUMNS {
            return Err(PowerSyncError::argument_error(format!(
                "Has more than {} columns, which is not supported",
                Self::MAX_AMOUNT_OF_COLUMNS
            )));
        }

        // TODO: check invalid chars in table and view override name

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

            if !column_names.insert(column.name) {
                return Err(PowerSyncError::argument_error(format!(
                    "Duplicate column: {}",
                    column.name
                )));
            }

            // TODO: Check invalid chars
        }

        let mut index_names = HashSet::new();
        for index in &self.indexes {
            if !index_names.insert(index.name) {
                return Err(PowerSyncError::argument_error(format!(
                    "Duplicate index: {}",
                    index.name
                )));
            }

            // TODO: Check invalid chars

            for column in &index.columns {
                if !column_names.contains(&column.name) {
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

impl<'a> Serialize for Table<'a> {
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
pub struct Column<'a> {
    pub name: &'a str,
    #[serde(rename = "type")]
    pub column_type: ColumnType,
}

impl<'a> Column<'a> {
    pub fn text(name: &'a str) -> Self {
        Self {
            name,
            column_type: ColumnType::Text,
        }
    }

    pub fn integer(name: &'a str) -> Self {
        Self {
            name,
            column_type: ColumnType::Integer,
        }
    }

    pub fn real(name: &'a str) -> Self {
        Self {
            name,
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
pub struct Index<'a> {
    pub name: &'a str,
    pub columns: Vec<IndexedColumn<'a>>,
}

#[derive(Serialize, Debug)]
pub struct IndexedColumn<'a> {
    pub name: &'a str,
    pub ascending: bool,
    #[serde(rename = "type")]
    pub type_name: &'a str,
}

#[derive(Serialize, Debug)]
pub struct RawTable<'a> {
    pub name: &'a str,
    pub put: PendingStatement<'a>,
    pub delete: PendingStatement<'a>,
}

#[derive(Serialize, Debug)]
pub struct PendingStatement<'a> {
    pub sql: String,
    /// This vec should contain an entry for each parameter in [sql].
    #[serde(borrow)]
    pub params: Vec<PendingStatementValue<'a>>,
}

#[derive(Serialize, Debug)]
pub enum PendingStatementValue<'a> {
    Id,
    Column(&'a str),
    // TODO: Stuff like a raw object of put data?
}

/// Options to include old values in CRUD entries for update statements.
///
/// These operations are enabled py passing them to a non-local [Table] constructor.
#[derive(Serialize, Debug)]
pub struct TrackPreviousValues<'a> {
    pub column_filter: Option<Vec<&'a str>>,
    pub only_when_changed: bool,
}

impl<'a> TrackPreviousValues<'a> {
    pub fn all() -> Self {
        Self {
            column_filter: None,
            only_when_changed: false,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::schema::{Table, TrackPreviousValues};

    #[test]
    fn handles_options_track_metadata() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.track_metadata = true
        }))
        .unwrap();

        assert_eq!(
            value
                .as_object()
                .unwrap()
                .get("include_metadata")
                .unwrap()
                .as_bool()
                .unwrap(),
            true
        );
    }

    #[test]
    fn handles_options_ignore_empty_updates() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.ignore_empty_updates = true
        }))
        .unwrap();

        assert_eq!(
            value
                .as_object()
                .unwrap()
                .get("ignore_empty_update")
                .unwrap()
                .as_bool()
                .unwrap(),
            true
        );
    }

    #[test]
    fn handles_options_track_previous_all() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.track_previous_values = Some(TrackPreviousValues::all())
        }))
        .unwrap();
        let value = value.as_object().unwrap();

        assert_eq!(value.get("include_old").unwrap().as_bool().unwrap(), true);
        assert_eq!(
            value
                .get("include_old_only_when_changed")
                .unwrap()
                .as_bool()
                .unwrap(),
            false
        );
    }

    #[test]
    fn handles_options_track_previous_column_filter() {
        let value = serde_json::to_value(Table::create("foo", vec![], |tbl| {
            tbl.track_previous_values = Some(TrackPreviousValues::all())
        }))
        .unwrap();
        let value = value.as_object().unwrap();

        assert_eq!(value.get("include_old").unwrap().as_bool().unwrap(), true);
        assert_eq!(
            value
                .get("include_old_only_when_changed")
                .unwrap()
                .as_bool()
                .unwrap(),
            false
        );
    }
}
