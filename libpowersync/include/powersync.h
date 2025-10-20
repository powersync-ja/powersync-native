#pragma once

#include "sqlite3.h"
#include <string>
#include <optional>
#include <utility>
#include <thread>
#include <vector>

namespace powersync {
  namespace internal {
    struct RawPowerSyncDatabase {
      void* sync;
      void* inner;
    };
  }

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

class Database {
private:
  internal::RawPowerSyncDatabase raw;
  std::optional<std::thread> worker;

  explicit Database(internal::RawPowerSyncDatabase raw) : raw(raw) {}

  // Databases can't be copied, the internal::RawPowerSyncDatabase is an exclusive reference in Rust.
  Database(const Database&) = delete;
public:
  void disconnect();
  void spawn_sync_thread();

  ~Database();

  [[nodiscard]] LeasedConnection reader() const;
  [[nodiscard]] LeasedConnection writer() const;

  static Database in_memory(const Schema& schema);
};

class Exception final : public std::exception {
private:
  const int rc;
  char* msg;
public:
  explicit Exception(int rc, char* msg) : rc(rc), msg(msg) {}

  [[nodiscard]] const char *what() const noexcept override;
  ~Exception() noexcept override;
};
}
