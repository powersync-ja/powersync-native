use eframe::egui;
use powersync::PowerSyncEnvironment;

use crate::ui::TodoListApp;

mod database;
mod ui;

fn main() -> eframe::Result {
    env_logger::init(); // Log to stderr (if you run with `RUST_LOG=debug`).
    PowerSyncEnvironment::powersync_auto_extension().expect("should load core extension");

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([580.0, 320.0]),
        ..Default::default()
    };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("should build runtime");

    let app = Box::new(TodoListApp::new(rt));

    eframe::run_native("PowerSync TodoList", options, Box::new(|_cc| Ok(app)))
}
