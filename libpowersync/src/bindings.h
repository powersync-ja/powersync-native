#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

namespace powersync::internal {

enum class ColumnType {
  Text = 0,
  Integer = 1,
  Real = 2,
};

enum class LogLevel {
  Error = 0,
  Warn = 1,
  Info = 2,
  Debug = 3,
  Trace = 4,
};

enum class PowerSyncResultCode {
  OK = 0,
  ERROR = 1,
};

struct RawConnectionLease;

using CppCompletionHandle = void*;

struct RawCrudTransaction {
  int64_t id;
  int64_t last_item_id;
  bool has_id;
  intptr_t crud_length;
};

struct StringView {
  const char *value;
  intptr_t length;
};

struct RawCrudEntry {
  int64_t client_id;
  int64_t transaction_id;
  int32_t update_type;
  StringView table;
  StringView id;
  StringView metadata;
  bool has_metadata;
  StringView data;
  bool has_data;
  StringView previous_values;
  bool has_previous_values;
};

struct Column {
  const char *name;
  ColumnType column_type;
};

struct Table {
  const char *name;
  const char *view_name_override;
  const Column *columns;
  uintptr_t column_len;
  bool local_only;
  bool insert_only;
  bool track_metadata;
  bool ignore_empty_updates;
};

struct RawSchema {
  const Table *tables;
  uintptr_t tables_len;
};

struct CppConnector {
  void (*upload_data)(CppConnector*, CppCompletionHandle);
  void (*fetch_credentials)(CppConnector*, CppCompletionHandle);
  void (*drop)(CppConnector*);
};

struct ConnectionLeaseResult {
  sqlite3 *sqlite3;
  RawConnectionLease *lease;
};

struct CppLogger {
  LogLevel level;
  void (*native_log)(LogLevel level, const char *line);
};

extern "C" {

void powersync_completion_handle_complete_credentials(CppCompletionHandle *handle,
                                                      const char *endpoint,
                                                      const char *token);

void powersync_completion_handle_complete_empty(CppCompletionHandle *handle);

void powersync_completion_handle_complete_error_code(CppCompletionHandle *handle, int code);

void powersync_completion_handle_complete_error_msg(CppCompletionHandle *handle,
                                                    int code,
                                                    const char *msg);

void powersync_completion_handle_free(CppCompletionHandle *handle);

void *powersync_crud_transactions_new(const RawPowerSyncDatabase *db);

PowerSyncResultCode powersync_crud_transactions_step(void *stream, bool *has_next);

RawCrudTransaction powersync_crud_transactions_current(const void *stream);

RawCrudEntry powersync_crud_transactions_current_crud_item(const void *stream, intptr_t index);

PowerSyncResultCode powersync_crud_complete(const RawPowerSyncDatabase *db,
                                            int64_t last_item_id,
                                            bool has_checkpoint,
                                            int64_t checkpoint);

void powersync_crud_transactions_free(void *stream);

PowerSyncResultCode powersync_db_in_memory(RawSchema schema, RawPowerSyncDatabase *out_db);

PowerSyncResultCode powersync_db_connect(const RawPowerSyncDatabase *db,
                                         const CppConnector *connector);

PowerSyncResultCode powersync_db_disconnect(const RawPowerSyncDatabase *db);

PowerSyncResultCode powersync_db_reader(const RawPowerSyncDatabase *db,
                                        ConnectionLeaseResult *out_lease);

PowerSyncResultCode powersync_db_writer(const RawPowerSyncDatabase *db,
                                        ConnectionLeaseResult *out_lease);

void powersync_db_return_lease(RawConnectionLease *lease);

void *powersync_db_watch_tables(const RawPowerSyncDatabase *db,
                                const StringView *tables,
                                uintptr_t table_count,
                                void (*listener)(const void*),
                                const void *token);

void powersync_db_watch_tables_end(void *watcher);

void powersync_db_free(RawPowerSyncDatabase db);

char *powersync_last_error_desc();

void powersync_free_str(const char *str);

/// Runs asynchronous PowerSync tasks on the current thread.
///
/// This blocks the thread until the database is closed.
void powersync_run_tasks(const RawPowerSyncDatabase *db);

int powersync_install_logger(CppLogger logger);

void *powersync_db_status(const RawPowerSyncDatabase *db);

void powersync_status_free(const void *status);

void *powersync_db_status_listener(const RawPowerSyncDatabase *db,
                                   void (*listener)(const void*),
                                   const void *token);

void powersync_db_status_listener_clear(void *listener);

}  // extern "C"

}  // namespace powersync::internal
