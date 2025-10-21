#include <iostream>

#include <curl/curl.h>
#include <nlohmann/json.hpp>
#include "powersync.h"

void check_rc(int rc) {
    if (rc != SQLITE_OK) {
        throw std::runtime_error("SQLite error: " + std::string(sqlite3_errstr(rc)));
    }
}

class DemoConnector: public powersync::BackendConnector {
private:
    std::shared_ptr<powersync::Database> database;

    static size_t write_callback(void *contents, size_t size, size_t nmemb, void *userp) {
        auto* response = static_cast<std::string*>(userp);
        response->append(static_cast<char *>(contents), size * nmemb);
        return size * nmemb;
    }
public:
    explicit DemoConnector(const std::shared_ptr<powersync::Database>& database): database(database) {}

    void fetch_token(powersync::CompletionHandle<powersync::PowerSyncCredentials> completion) override {
        std::thread([completion = std::move(completion)]() mutable {
            using json = nlohmann::json;

            const auto curl = curl_easy_init();
            std::string response;

            curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:6060/api/auth/token");
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

            if (auto res = curl_easy_perform(curl); res != CURLE_OK) {
                completion.complete_error(res, "CURL request failed");
                return;
            }
            curl_easy_cleanup(curl);
            json parsed_response = response;

            std::string token = parsed_response["token"];
            completion.complete_ok(powersync::PowerSyncCredentials {
                .endpoint = "http://localhost:8080/",
                .token = token,
            });
        }).detach();
    }

    void upload_data(powersync::CompletionHandle<std::monostate> completion) override {
        completion.complete_ok(std::monostate());
    }

    ~DemoConnector() override = default;
};

int main() {
    using namespace powersync;
    set_logger(LogLevel::Trace, [](LogLevel _, const char* message) {
        std::cout << message << std::endl;
    });

    Schema schema{};
    schema.tables.emplace_back(Table{"users", {
        Column::text("name")
    }});

    auto db = std::make_shared<Database>(std::move(Database::in_memory(schema)));
    db->spawn_sync_thread();
    auto connector = std::make_shared<DemoConnector>(db);
    db->connect(connector);

    {
        auto writer = db->writer();
        check_rc(sqlite3_exec(writer, "INSERT INTO users (id, name) VALUES (uuid(), 'Simon');", nullptr, nullptr, nullptr));
    }

    {
        auto reader = db->reader();
        sqlite3_stmt *stmt = nullptr;
        check_rc(sqlite3_prepare_v2(reader, "SELECT id, name FROM users", -1, &stmt, nullptr));

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::cout << sqlite3_column_text(stmt, 0) << ": " << sqlite3_column_text(stmt, 1) << std::endl;
        }
        sqlite3_finalize(stmt);
    }
}
