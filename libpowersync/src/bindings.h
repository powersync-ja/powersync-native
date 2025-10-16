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

struct RawPowerSyncDatabase {
  InnerPowerSyncState *db;
};

struct ConnectionLeaseResult {
  sqlite3 *sqlite3;
  RawConnectionLease *lease;
};

extern "C" {

PowerSyncResultCode powersync_db_in_memory(RawSchema schema, RawPowerSyncDatabase *out_db);

PowerSyncResultCode powersync_db_reader(const InnerPowerSyncState *db,
                                        ConnectionLeaseResult *out_lease);

PowerSyncResultCode powersync_db_writer(const InnerPowerSyncState *db,
                                        ConnectionLeaseResult *out_lease);

void powersync_db_return_lease(RawConnectionLease *lease);

void powersync_db_free(RawPowerSyncDatabase db);

char *powersync_last_error_desc();

void powersync_free_str(char *str);

/// Runs asynchronous PowerSync tasks on the current thread.
///
/// This blocks the thread until the database is closed.
void powersync_run_tasks(const InnerPowerSyncState *db);

}  // extern "C"

}  // namespace powersync::internal
