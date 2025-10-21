macro_rules! ps_try {
    ($result:expr) => {
        match $result {
            Ok(value) => value,
            Err(e) => return e.into(),
        }
    };
}

mod completion_handle;
mod connector;
mod database;
mod error;
mod executor;
mod schema;
