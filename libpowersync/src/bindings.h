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

enum class PowerSyncResultCode {
  OK = 0,
  ERROR = 1,
};

struct RawConnectionLease;

using CppCompletionHandle = void*;

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

PowerSyncResultCode powersync_db_in_memory(RawSchema schema, RawPowerSyncDatabase *out_db);

PowerSyncResultCode powersync_db_connect(const RawPowerSyncDatabase *db,
                                         const CppConnector *connector);

PowerSyncResultCode powersync_db_reader(const RawPowerSyncDatabase *db,
                                        ConnectionLeaseResult *out_lease);

PowerSyncResultCode powersync_db_writer(const RawPowerSyncDatabase *db,
                                        ConnectionLeaseResult *out_lease);

void powersync_db_return_lease(RawConnectionLease *lease);

void powersync_db_free(RawPowerSyncDatabase db);

char *powersync_last_error_desc();

void powersync_free_str(char *str);

/// Runs asynchronous PowerSync tasks on the current thread.
///
/// This blocks the thread until the database is closed.
void powersync_run_tasks(const RawPowerSyncDatabase *db);

}  // extern "C"

}  // namespace powersync::internal
