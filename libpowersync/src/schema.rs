use powersync::schema as ps;
use std::borrow::Cow;

use std::ffi::{CStr, c_char};

#[repr(C)]
enum ColumnType {
    Text = 0,
    Integer = 1,
    Real = 2,
}

#[repr(C)]
pub struct Column {
    name: *const c_char,
    column_type: ColumnType,
}

fn copy_string(ptr: *const c_char) -> String {
    unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_string()
}

fn copy_nullable_string(ptr: *const c_char) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        Some(copy_string(ptr))
    }
}

impl Column {
    pub fn copy_to_rust(&self) -> ps::Column {
        ps::Column {
            name: Cow::Owned(copy_string(self.name)),
            column_type: match self.column_type {
                ColumnType::Text => ps::ColumnType::Text,
                ColumnType::Integer => ps::ColumnType::Integer,
                ColumnType::Real => ps::ColumnType::Real,
            },
        }
    }
}

#[repr(C)]
pub struct Table {
    name: *const c_char,
    view_name_override: *const c_char,
    columns: *const Column,
    column_len: usize,
    local_only: bool,
    insert_only: bool,
    track_metadata: bool,
    ignore_empty_updates: bool,
}

impl Table {
    fn columns(&self) -> &[Column] {
        unsafe { std::slice::from_raw_parts(self.columns, self.column_len) }
    }

    pub fn copy_to_rust(&self) -> ps::Table {
        ps::Table {
            name: Cow::Owned(copy_string(self.name)),
            view_name_override: copy_nullable_string(self.view_name_override).map(Cow::from),
            columns: self.columns().iter().map(|c| c.copy_to_rust()).collect(),
            indexes: vec![],
            local_only: self.local_only,
            insert_only: self.insert_only,
            track_metadata: self.track_metadata,
            track_previous_values: None,
            ignore_empty_updates: self.ignore_empty_updates,
        }
    }
}

#[repr(C)]
pub struct RawSchema {
    tables: *const Table,
    tables_len: usize,
}

impl RawSchema {
    fn tables(&self) -> &[Table] {
        unsafe { std::slice::from_raw_parts(self.tables, self.tables_len) }
    }

    pub fn copy_to_rust(&self) -> ps::Schema {
        let mut schema = ps::Schema::default();
        for table in self.tables() {
            schema.tables.push(table.copy_to_rust());
        }

        schema
    }
}
