use log::{Level, LevelFilter, Metadata, Record};
use std::ffi::{CString, c_char, c_int};

#[unsafe(no_mangle)]
pub extern "C" fn powersync_install_logger(logger: CppLogger) -> c_int {
    let level = logger.level.into();
    match log::set_boxed_logger(Box::new(logger)).map(|()| log::set_max_level(level)) {
        Ok(_) => 0,
        Err(e) => 1,
    }
}

#[derive(PartialOrd, Ord, Eq, PartialEq, Copy, Clone)]
#[repr(C)]
pub enum LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
}

impl From<Level> for LogLevel {
    fn from(value: Level) -> Self {
        match value {
            Level::Error => Self::Error,
            Level::Warn => Self::Warn,
            Level::Info => Self::Info,
            Level::Debug => Self::Debug,
            Level::Trace => Self::Trace,
        }
    }
}

impl From<LogLevel> for LevelFilter {
    fn from(value: LogLevel) -> Self {
        match value {
            LogLevel::Error => Self::Error,
            LogLevel::Warn => Self::Warn,
            LogLevel::Info => Self::Info,
            LogLevel::Debug => Self::Debug,
            LogLevel::Trace => Self::Trace,
        }
    }
}

#[repr(C)]
pub struct CppLogger {
    level: LogLevel,
    native_log: extern "C" fn(level: LogLevel, line: *const c_char),
}

impl log::Log for CppLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        self.level >= metadata.level().into()
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            let line = format!("{}: {}", record.target(), record.args());

            if let Ok(line) = CString::new(line) {
                (self.native_log)(record.level().into(), line.as_ptr())
            } else {
                // This would be an error if the log line contained null bytes. We can't really log
                // them in that case.
                (self.native_log)(
                    LogLevel::Error,
                    c"Tried logging a line with a null char.".as_ptr(),
                );
            }
        }
    }

    fn flush(&self) {}
}
