#include <iostream>

#include "powersync.h"

void check_rc(int rc) {
    if (rc != SQLITE_OK) {
        throw std::runtime_error("SQLite error: " + std::string(sqlite3_errstr(rc)));
    }
}

int main() {
    using namespace powersync;

    Schema schema{};
    schema.tables.emplace_back(Table{"users", {
        Column::text("name")
    }});
    auto db = Database::in_memory(schema);

    {
        auto writer = db.writer();
        check_rc(sqlite3_exec(writer, "INSERT INTO users (id, name) VALUES (uuid(), 'Simon');", nullptr, nullptr, nullptr));
    }

    {
        auto reader = db.reader();
        sqlite3_stmt *stmt = nullptr;
        check_rc(sqlite3_prepare_v2(reader, "SELECT id, name FROM users", -1, &stmt, nullptr));

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::cout << sqlite3_column_text(stmt, 0) << ": " << sqlite3_column_text(stmt, 1) << std::endl;
        }
        sqlite3_finalize(stmt);
    }
}
