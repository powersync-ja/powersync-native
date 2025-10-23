#pragma once

#include "sqlite3.h"
#include <string>
#include <optional>
#include <utility>
#include <chrono>
#include <thread>
#include <vector>

namespace powersync {
  namespace internal {
    struct RawPowerSyncDatabase {
      void* sync;
      void* inner;
    };

    struct RawCompletionHandle {
      void* rust_handle;

      void send_empty();
      void send_credentials(const char* endpoint, const char* token);
      void send_error_code(int code);
      void send_error_message(int code, const char* message);

      ~RawCompletionHandle();
    };
  }

  enum class UpdateType {
    PUT = 1,
    PATCH = 2,
    DELETE = 3,
  };

  struct CrudEntry {
    int64_t client_id;
    int64_t transaction_id;
    UpdateType update_type;
    std::string table;
    std::string id;
    std::optional<std::string> metadata;
    std::optional<std::string> data;
    std::optional<std::string> previous_values;
  };

  class Database;

  class CrudTransaction {
  public:
    const Database& db;
    int64_t last_item_id;
    std::optional<int64_t> id;
    std::vector<CrudEntry> crud;

    void complete() const;
    void complete(std::optional<int64_t> custom_write_checkpoint) const;
  };

  enum class LogLevel {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
  };

  void set_logger(LogLevel level, void(*logger)(LogLevel, const char*));

  enum ColumnType {
    TEXT,
    INTEGER,
    REAL
};

  struct Column {
    std::string name;
    ColumnType type;

    static Column text(std::string name) {
      return {.name = std::move(name), .type = TEXT};
    }
    static Column real(std::string name) {
      return {.name = std::move(name), .type = REAL};
    }
    static Column integer(std::string name) {
      return {.name = std::move(name), .type = INTEGER};
    }
  };

  struct Table {
    std::string name;
    std::vector<Column> columns;
    //std::vector<Index> indices = {};
    bool local_only = false;
    bool insert_only = false;
    std::optional<std::string> view_name_override = std::nullopt;
    bool track_metadata = false;
    bool ignore_empty_updates = false;
    //std::optional<TrackPreviousValues> track_previous_values = std::nullopt;

    Table(std::string name, std::vector<Column> columns): name(std::move(name)), columns(std::move(columns)) {}
  };

struct Schema {
  std::vector<Table> tables;
};

struct LeasedConnection {
private:
  sqlite3* db;
  void* raw_lease;
  LeasedConnection(sqlite3* db, void* raw_lease)
    :db(db), raw_lease(raw_lease) {};

public:
  friend class Database;

  ~LeasedConnection();
  operator sqlite3*() const;
};

template <typename T>
struct CompletionHandle {
private:
  internal::RawCompletionHandle handle;

public:
  explicit CompletionHandle(internal::RawCompletionHandle handle): handle(std::move(handle)) {}

  void complete_ok(T result);

  void complete_error(int code) {
    this->handle.send_error_code(code);
  }

  void complete_error(int code, const std::string& description) {
    this->handle.send_error_message(code, description.c_str());
  }
};

template<>
inline void CompletionHandle<std::monostate>::complete_ok(std::monostate _result) {
  this->handle.send_empty();
}

struct PowerSyncCredentials {
  const std::string& endpoint;
  const std::string& token;
};

template<>
inline void CompletionHandle<PowerSyncCredentials>::complete_ok(PowerSyncCredentials credentials) {
  this->handle.send_credentials(credentials.endpoint.c_str(), credentials.token.c_str());
}

/// Note that methods on this class may be called from multiple threads, or concurrently. Backend connectors must thus
/// be thread-safe.
class BackendConnector {
public:
  virtual void fetch_token(CompletionHandle<PowerSyncCredentials> completion) {}
  virtual void upload_data(CompletionHandle<std::monostate> completion) {}

  virtual ~BackendConnector() = default;
};

class CrudTransactions {
  const Database& db;
  void* rust_iterator;

  CrudTransactions(const Database& db, void* rust_iterator): db(db), rust_iterator(rust_iterator) {}

  // The rust iterator can't be copied.
  CrudTransactions(const CrudTransactions&) = delete;

  friend class Database;
public:
  CrudTransactions(CrudTransactions&& other) noexcept:
    db(other.db),
    rust_iterator(other.rust_iterator) {
    other.rust_iterator = nullptr;
  }

  bool advance();
  CrudTransaction current() const;

  ~CrudTransactions();
};

/// An active watcher on a [Database].
///
/// Calling the destructor of watchers will unregister the listener.
class Watcher {
  void* rust_status_watcher;
  void* rust_table_watcher;
  std::function<void()> callback;

  friend class Database;
  static void dispatch(const void* token);

  public:
  explicit Watcher(std::function<void()> callback): rust_status_watcher(nullptr), rust_table_watcher(nullptr), callback(callback) {}

  Watcher(const Watcher&) = delete;
  Watcher(Watcher&& a) noexcept : rust_status_watcher(a.rust_status_watcher), rust_table_watcher(a.rust_table_watcher), callback(std::move(a.callback)) {
    a.rust_status_watcher = nullptr;
    a.rust_table_watcher = nullptr;
  }

  ~Watcher();
};

class SyncStream;
class SyncStreamSubscription;

/// Information about a progressing download.
///
/// This reports the `total` amount of operations to download, how many of them have already been
/// `downloaded`, and finally a `fraction()` indicating relative progress.
struct ProgressCounters {
  /// How many operations need to be downloaded in total for the current download to complete.
  int64_t total;
  /// How many operations, out of `total`, have already been downloaded.
  int64_t downloaded;

  /// The relative amount of `total` to items in `downloaded`, as a number between `0.0` and `1.0` (inclusive).
  ///
  /// When this number reaches `1.0`, all changes have been received from the sync service. Actually applying these
  /// changes happens before the progress is cleared though, so progress can stay at `1.0` for a short while before
  /// completing.
  float fraction() const {
    if (total == 0) {
      return 0;
    }
    return static_cast<float>(downloaded) / static_cast<float>(total);
  }
};

struct SyncStreamStatus {
  std::string name;
  std::optional<std::string> parameters;

  std::optional<ProgressCounters> progress;
  bool is_active;
  bool is_default;
  bool has_explicit_subscription;
  std::optional<std::chrono::time_point<std::chrono::system_clock>> expires_at;
  bool has_synced;
  std::optional<std::chrono::time_point<std::chrono::system_clock>> last_synced_at;
};

class SyncStatus {
  void* rust_status;

  void read();

  explicit SyncStatus(void* rust_status);
  friend class Database;
public:
  bool connected;
  bool connecting;
  bool downloading;
  std::optional<std::string> download_error;

  bool uploading;
  std::optional<std::string> upload_error;

  SyncStatus(const SyncStatus& other);
  SyncStatus(SyncStatus&& other) = delete;

  std::optional<SyncStreamStatus> for_stream(const SyncStream& stream) const;
  std::vector<SyncStreamStatus> all_streams() const;

  friend std::ostream& operator<<(std::ostream& os, const SyncStatus& status);

  ~SyncStatus();
};

class Database {
  internal::RawPowerSyncDatabase raw;
  std::optional<std::thread> worker;

  explicit Database(internal::RawPowerSyncDatabase raw) : raw(raw) {}

  // Databases can't be copied, the internal::RawPowerSyncDatabase is an exclusive reference in Rust.
  Database(const Database&) = delete;

  friend class CrudTransaction;
public:
  Database(Database&& other) noexcept:
    raw(other.raw),
    worker(std::move(other.worker)) {
    other.raw.inner = nullptr;
    other.raw.sync = nullptr;
  }

  void connect(std::shared_ptr<BackendConnector> connector);
  void disconnect();
  void spawn_sync_thread();

  /// Returns an iterator of completed crud transactions made against this database.
  ///
  /// This database must outlive the returned transactions stream.
  CrudTransactions get_crud_transactions() const;

  SyncStatus sync_status() const;

  /// The watcher keeps a reference to the current database, which must outlive it.
  [[nodiscard]] std::unique_ptr<Watcher> watch_sync_status(std::function<void()> callback) const;
  [[nodiscard]] std::unique_ptr<Watcher> watch_tables(const std::initializer_list<std::string>& tables, std::function<void ()> callback) const;

  [[nodiscard]] LeasedConnection reader() const;
  [[nodiscard]] LeasedConnection writer() const;

  ~Database();

  static Database in_memory(const Schema& schema);
};

class SyncStream {
  const Database& db;
public:
  const std::string name;
  const std::optional<std::string> parameters;

  SyncStream(const Database& db, const std::string& name): db(db), name(name) {}
  SyncStream(const Database& db, const std::string& name, std::string parameters): db(db), name(name), parameters(parameters) {}

  SyncStreamSubscription subscribe();
};

class SyncStreamSubscription {
private:
  void* rust_subscription;
public:
  const SyncStream stream;
};

class Exception final : public std::exception {
private:
  const int rc;
  const char* msg;
public:
  explicit Exception(int rc, const char* msg) : rc(rc), msg(msg) {}

  [[nodiscard]] const char *what() const noexcept override;
  ~Exception() noexcept override;
};
}
