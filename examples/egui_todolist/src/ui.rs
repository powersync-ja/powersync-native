use std::sync::{Arc, Mutex};

use eframe::egui;
use futures_lite::StreamExt;
use powersync::SyncStatusData;
use tokio::runtime::Runtime;

use crate::database::{TodoDatabase, TodoList};

pub struct TodoListApp {
    rt: Runtime,
    shared: Arc<SharedTodoListState>,
    has_tasks: bool,
}

struct SharedTodoListState {
    db: TodoDatabase,
    sync_state: Mutex<Arc<SyncStatusData>>,
    lists: Mutex<Vec<TodoList>>,
}

impl TodoListApp {
    pub fn new(rt: Runtime) -> Self {
        let db = TodoDatabase::new(&rt);

        Self {
            rt,
            shared: Arc::new(SharedTodoListState {
                sync_state: Mutex::new(db.db.status()),
                db: db,
                lists: Default::default(),
            }),
            has_tasks: false,
        }
    }
}

impl eframe::App for TodoListApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        if !self.has_tasks {
            let state = self.shared.clone();
            let ctx = ctx.clone();
            self.rt.spawn({
                let state = self.shared.clone();
                async move {
                    let mut status = state.db.db.watch_status();
                    while let Some(status) = status.next().await {
                        let mut guard = state.sync_state.lock().unwrap();
                        *guard = status;
                        ctx.request_repaint();
                    }
                }
            });

            self.rt.spawn(async move {
                let mut stream = state.db.db.watch_tables(true, ["lists"]);
                while let Some(_) = stream.next().await {
                    let reader = state.db.db.reader().await.expect("get reader");
                    let items = TodoList::fetch_all(&reader).expect("fetch lists");

                    let mut guard = state.lists.lock().unwrap();
                    *guard = items;
                }
            });

            self.has_tasks = true;
        }

        let sync_status = {
            let guard = self.shared.sync_state.lock().unwrap();
            guard.clone()
        };

        let lists_stream = self.shared.db.db.sync_stream("lists", None);
        let has_lists = {
            let lists_status = sync_status.for_stream(&lists_stream);
            match lists_status {
                Some(status) => status.subscription.has_synced(),
                None => false,
            }
        };

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("PowerSync TodoList demo");

            ui.horizontal(|ui| {
                ui.label(format!(
                    "Sync status: Connected {}, downloading {}. Download error: {:?}",
                    sync_status.is_connected(),
                    sync_status.is_downloading(),
                    sync_status.download_error(),
                ));

                if sync_status.is_connected() || sync_status.is_connecting() {
                    if ui.button("Disconnect").clicked() {
                        let state = self.shared.clone();
                        self.rt.spawn(async move { state.db.disconnect().await });
                    }
                } else {
                    if ui.button("Connect").clicked() {
                        let state = self.shared.clone();
                        self.rt.spawn(async move { state.db.connect().await });
                    }
                }
            });

            ui.separator();

            if !has_lists {
                ui.label("Waiting to sync lists");
            } else {
                let lists = self.shared.lists.lock().unwrap();
                for list in &*lists {
                    ui.horizontal(|ui| {
                        ui.heading(&list.name);
                        if ui.button("Open").clicked() {
                            // TODO: Navigate to list
                        }
                    });
                    ui.separator();
                }
            }
        });
    }
}
