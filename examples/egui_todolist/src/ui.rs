use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use eframe::egui::{self, Ui};
use futures_lite::StreamExt;
use log::info;
use powersync::{CheckpointError, SyncStatusData};
use rusqlite::params;
use serde_json::{Map, Value};
use tokio::{runtime::Runtime, task::JoinHandle};

use crate::database::{TodoDatabase, TodoEntry, TodoList};

pub struct TodoListApp {
    rt: Runtime,
    shared: Arc<SharedTodoListState>,
    has_tasks: bool,
}

struct SharedTodoListState {
    db: TodoDatabase,
    sync_state: Mutex<Arc<SyncStatusData>>,
    lists: Mutex<Vec<TodoList>>,
    selected_list: Mutex<Option<SelectedTodoList>>,
    checkpoint_request: Mutex<CheckpointRequestState>,
}

#[derive(Default)]
enum CheckpointRequestState {
    #[default]
    Idle,
    InProgress,
    Succeeded(Instant),
    Failed(String),
}

const SUCCESS_DISPLAY_DURATION: Duration = Duration::from_secs(3);

struct SelectedTodoList {
    id: String,
    stream_params: Value,
    items: Vec<TodoEntry>,
    task: JoinHandle<()>,
}

impl Drop for SelectedTodoList {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TodoListApp {
    pub fn new(rt: Runtime) -> Self {
        let db = TodoDatabase::new();

        Self {
            rt,
            shared: Arc::new(SharedTodoListState {
                sync_state: Mutex::new(db.db.status()),
                db,
                lists: Default::default(),
                selected_list: Default::default(),
                checkpoint_request: Default::default(),
            }),
            has_tasks: false,
        }
    }
}

impl eframe::App for TodoListApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if !self.has_tasks {
            let state = self.shared.clone();
            let ui = ui.clone();
            self.rt.spawn({
                let state = self.shared.clone();
                async move {
                    let mut status = state.db.db.watch_status();
                    while let Some(status) = status.next().await {
                        info!("Sync status changed to {:?}", status);
                        let mut guard = state.sync_state.lock().unwrap();
                        *guard = status;
                        ui.request_repaint();
                    }
                }
            });

            self.rt.spawn(async move {
                let mut stream = state.db.db.watch_tables(true, ["lists"]);
                while stream.next().await.is_some() {
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

        egui::CentralPanel::default().show(ui, |ui| {
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
                        *self.shared.checkpoint_request.lock().unwrap() =
                            CheckpointRequestState::Idle;

                        let state = self.shared.clone();
                        self.rt.spawn(async move { state.db.disconnect().await });
                    }

                    let in_progress = matches!(
                        &*self.shared.checkpoint_request.lock().unwrap(),
                        CheckpointRequestState::InProgress
                    );

                    if ui
                        .add_enabled(!in_progress, egui::Button::new("Refresh"))
                        .clicked()
                    {
                        *self.shared.checkpoint_request.lock().unwrap() =
                            CheckpointRequestState::InProgress;

                        let state = self.shared.clone();
                        let ctx = ui.ctx().clone();
                        self.rt.spawn(async move {
                            let result: Result<(), CheckpointError> = async {
                                let request = state.db.db.request_checkpoint().await?;
                                request.wait_for_sync().await
                            }
                            .await;

                            *state.checkpoint_request.lock().unwrap() = match result {
                                Ok(()) => CheckpointRequestState::Succeeded(Instant::now()),
                                Err(err) => CheckpointRequestState::Failed(err.to_string()),
                            };
                            ctx.request_repaint();
                        });
                    }

                    // Expire the "Succeeded" state a few seconds after it was set.
                    {
                        let mut guard = self.shared.checkpoint_request.lock().unwrap();
                        if let CheckpointRequestState::Succeeded(at) = *guard
                            && at.elapsed() >= SUCCESS_DISPLAY_DURATION
                        {
                            *guard = CheckpointRequestState::Idle;
                        }
                    }

                    match &*self.shared.checkpoint_request.lock().unwrap() {
                        CheckpointRequestState::Idle => {}
                        CheckpointRequestState::InProgress => {
                            ui.spinner();
                            ui.label("Requesting checkpoint...");
                        }
                        CheckpointRequestState::Succeeded(at) => {
                            ui.colored_label(egui::Color32::GREEN, "Up to date");
                            ui.ctx()
                                .request_repaint_after(SUCCESS_DISPLAY_DURATION - at.elapsed());
                        }
                        CheckpointRequestState::Failed(err) => {
                            ui.colored_label(egui::Color32::RED, format!("Refresh failed: {err}"));
                        }
                    }
                } else if ui.button("Connect").clicked() {
                    let state = self.shared.clone();
                    self.rt.spawn(async move { state.db.connect().await });
                }
            });

            ui.separator();

            let mut content = TodoAppContent {
                ctx: &ui.clone(),
                app: self,
                status: sync_status,
                selected_list: self.shared.selected_list.lock().unwrap(),
            };

            ui.scope(|ui| {
                content.build_content(ui);
            });
        });
    }
}

struct TodoAppContent<'a> {
    ctx: &'a egui::Context,
    app: &'a TodoListApp,
    status: Arc<SyncStatusData>,
    selected_list: MutexGuard<'a, Option<SelectedTodoList>>,
}

impl<'a> TodoAppContent<'a> {
    fn navigate_to_list(&mut self, list: &TodoList) {
        let list_id = list.id.clone();
        let stream_params = Value::Object({
            let mut map = Map::new();
            map.insert("list".to_string(), Value::String(list_id.clone()));
            map
        });

        let task = self.app.rt.spawn({
            let state = self.app.shared.clone();
            let stream_params = stream_params.clone();
            let ctx = self.ctx.clone();

            async move {
                let stream = state.db.db.sync_stream("todos", Some(&stream_params));
                let _subscription = stream.subscribe().await.unwrap();

                let mut stream = state.db.db.watch_tables(true, ["todos"]);
                while stream.next().await.is_some() {
                    let reader = state.db.db.reader().await.expect("get reader");
                    let items = TodoEntry::fetch_in_list(&reader, &list_id).expect("fetch todos");

                    let mut guard = state.selected_list.lock().unwrap();
                    if let Some(selected) = &mut *guard
                        && selected.id == list_id
                    {
                        selected.items = items;
                        ctx.request_repaint();
                    }
                }
            }
        });

        *self.selected_list = Some(SelectedTodoList {
            id: list.id.clone(),
            stream_params,
            items: vec![],
            task,
        });
    }

    fn navigate_home(&mut self) {
        *self.selected_list = None;
    }

    fn build_home_page(&mut self, ui: &mut Ui) {
        let lists_stream = self.app.shared.db.db.sync_stream("lists", None);
        let has_lists = {
            let lists_status = self.status.for_stream(&lists_stream);
            match lists_status {
                Some(status) => status.subscription.has_synced(),
                None => false,
            }
        };

        if !has_lists {
            ui.label("Waiting to sync lists");
        } else {
            let lists = self.app.shared.lists.lock().unwrap();
            for list in &*lists {
                ui.horizontal(|ui| {
                    ui.heading(&list.name);
                    if ui.button("Open").clicked() {
                        self.navigate_to_list(list);
                    }
                });
                ui.separator();
            }
        }
    }

    fn build_details_page(&self, state: &SelectedTodoList, ui: &mut Ui) -> bool {
        let mut navigate_back_requested = false;
        let stream = self
            .app
            .shared
            .db
            .db
            .sync_stream("todos", Some(&state.stream_params));
        let did_sync = {
            let lists_status = self.status.for_stream(&stream);
            match lists_status {
                Some(status) => status.subscription.has_synced(),
                None => false,
            }
        };

        let name = {
            let lists = self.app.shared.lists.lock().unwrap();
            let mut name = None;
            for list in &*lists {
                if list.id == state.id {
                    name = Some(list.name.clone());
                    break;
                }
            }

            name
        };

        ui.horizontal(|ui| {
            if ui.button("Back to overview").clicked() {
                navigate_back_requested = true;
            }

            ui.heading(name.as_deref().unwrap_or("Unknown list"));
        });

        if did_sync {
            for item in &state.items {
                let mut checked = item.completed;

                if ui.checkbox(&mut checked, &item.description).changed() {
                    let id = item.id.clone();
                    let state = self.app.shared.clone();

                    self.app.rt.spawn(async move {
                        let writer = state.db.db.writer().await.unwrap();
                        writer
                            .execute(
                                "UPDATE todos SET completed = ? WHERE id = ?",
                                params![checked, id],
                            )
                            .unwrap();
                    });
                }
            }
        } else {
            ui.label("Waiting to sync contents for this list");
        }

        navigate_back_requested
    }

    fn build_content(&mut self, ui: &mut Ui) {
        if let Some(list) = &*self.selected_list {
            let navigate_back = self.build_details_page(list, ui);
            if navigate_back {
                self.navigate_home();
            }
        } else {
            self.build_home_page(ui);
        }
    }
}
