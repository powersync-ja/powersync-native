use crate::error::PowerSyncResultCode;
use futures_lite::{Stream, StreamExt};
use powersync::error::PowerSyncError;
use powersync::ffi::RawPowerSyncDatabase;
use powersync::{CrudTransaction, UpdateType};
use std::ffi::{c_char, c_void};
use std::pin::Pin;
use std::ptr::null_mut;

#[repr(C)]
pub struct RawCrudEntry {
    pub client_id: i64,
    pub transaction_id: i64,
    pub update_type: i32,
    pub table: StringView,
    pub id: StringView,
    pub metadata: StringView,
    pub has_metadata: bool,
    pub data: StringView,
    pub has_data: bool,
    pub previous_values: StringView,
    pub has_previous_values: bool,
}

#[repr(C)]
pub struct StringView {
    pub value: *const c_char,
    pub length: isize,
}

impl StringView {
    pub fn view(source: &str) -> Self {
        Self {
            value: source.as_ptr().cast(),
            length: source.len() as isize,
        }
    }

    pub fn view_optional(source: Option<&str>) -> Self {
        match source {
            Some(str) => Self::view(str),
            None => Self {
                value: null_mut(),
                length: 0,
            },
        }
    }
}

impl AsRef<str> for StringView {
    fn as_ref(&self) -> &str {
        let bytes =
            unsafe { std::slice::from_raw_parts(self.value as *const u8, self.length as usize) };
        std::str::from_utf8(bytes).unwrap()
    }
}

#[repr(C)]
pub struct RawCrudTransaction {
    pub id: i64,
    pub last_item_id: i64,
    pub has_id: bool,
    pub crud_length: isize,
}

struct RawTransactionStream<'a> {
    stream: Pin<Box<dyn Stream<Item = Result<CrudTransaction<'a>, PowerSyncError>> + Send + 'a>>,
    current: Option<CrudTransaction<'a>>,
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_crud_transactions_new(db: &RawPowerSyncDatabase) -> *mut c_void {
    let stream = RawTransactionStream {
        stream: db.crud_transactions().boxed(),
        current: None,
    };

    Box::into_raw(Box::new(stream)) as *mut c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_crud_transactions_step(
    stream: *mut c_void,
    has_next: &mut bool,
) -> PowerSyncResultCode {
    let stream = unsafe { &mut *(stream as *mut RawTransactionStream) };
    let result = ps_try!(futures_lite::future::block_on(stream.stream.try_next()));

    match result {
        None => *has_next = false,
        Some(result) => {
            *has_next = true;
            stream.current = Some(result);
        }
    };
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_crud_transactions_current(stream: *const c_void) -> RawCrudTransaction {
    let stream = unsafe { &*(stream as *const RawTransactionStream) };
    let item = stream.current.as_ref().unwrap();

    RawCrudTransaction {
        id: item.id.unwrap_or_default(),
        last_item_id: item.last_item_id,
        has_id: item.id.is_some(),
        crud_length: item.crud.len() as isize,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_crud_transactions_current_crud_item(
    stream: *const c_void,
    index: isize,
) -> RawCrudEntry {
    let stream = unsafe { &*(stream as *const RawTransactionStream) };
    let item = stream.current.as_ref().unwrap();
    let item = &item.crud[index as usize];

    RawCrudEntry {
        client_id: item.client_id,
        transaction_id: item.transaction_id,
        update_type: match item.update_type {
            // Must match enum class UpdateType from include/powersync.h
            UpdateType::Put => 1,
            UpdateType::Patch => 2,
            UpdateType::Delete => 3,
        },
        table: StringView::view(&item.table),
        id: StringView::view(&item.id),
        metadata: StringView::view_optional(item.metadata.as_deref()),
        has_metadata: item.metadata.is_some(),
        data: StringView::view_optional(item.raw_data.as_deref()),
        has_data: item.data.is_some(),
        previous_values: StringView::view_optional(item.raw_previous_values.as_deref()),
        has_previous_values: item.previous_values.is_some(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_crud_complete(
    db: &RawPowerSyncDatabase,
    last_item_id: i64,
    has_checkpoint: bool,
    checkpoint: i64,
) -> PowerSyncResultCode {
    let future = db.complete_crud_items(
        last_item_id,
        if has_checkpoint {
            Some(checkpoint)
        } else {
            None
        },
    );
    ps_try!(futures_lite::future::block_on(future));
    PowerSyncResultCode::OK
}

#[unsafe(no_mangle)]
pub extern "C" fn powersync_crud_transactions_free(stream: *mut c_void) {
    drop(unsafe { Box::from_raw(stream as *mut RawTransactionStream) })
}
