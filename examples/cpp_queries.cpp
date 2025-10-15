#include <iostream>

#include "powersync.h"

void check_rc(int rc) {
    if (rc != SQLITE_OK) {
        throw std::runtime_error("SQLite error: " + std::string(sqlite3_errstr(rc)));
    }
}

int main() {
    using namespace powersync;

    constexpr Schema schema{};
    auto db = Database::in_memory(schema);

    {
        auto writer = db.writer();
        check_rc(sqlite3_exec(writer, "CREATE TABLE foo (bar TEXT) STRICT;", nullptr, nullptr, nullptr));
        check_rc(sqlite3_exec(writer, "INSERT INTO foo (bar) VALUES ('testing');", nullptr, nullptr, nullptr));
    }

    {
        auto reader = db.reader();
        sqlite3_stmt *stmt = nullptr;
        check_rc(sqlite3_prepare_v2(reader, "SELECT * FROM foo", -1, &stmt, nullptr));

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::cout << sqlite3_column_text(stmt, 0) << std::endl;
        }
        sqlite3_finalize(stmt);
    }
}
