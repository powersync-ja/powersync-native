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
            json parsed_response = json::parse(response);

            std::string token = parsed_response["token"];
            completion.complete_ok(powersync::PowerSyncCredentials {
                .endpoint = "http://localhost:8080/",
                .token = token,
            });
        }).detach();
    }

    void upload_data(powersync::CompletionHandle<std::monostate> completion) override {
        const auto db = this->database;
        std::thread([db, completion = std::move(completion)]() mutable {
            std::cout << "Starting crud uploads" << std::endl;

            auto transactions = db->get_crud_transactions();
            while (transactions.advance()) {
                using json = nlohmann::json;

                auto tx = transactions.current();
                json entries = json::array({});

                std::cout << "Has transaction, id " << *tx.id << std::endl;
                for (const auto& item : tx.crud) {
                    std::cout << "Has item: " << item.table << ": " << item.id << std::endl;

                    json entry;
                    switch (item.update_type) {
                        case powersync::UpdateType::PUT:
                            entry["op"] = "PUT";
                            break;
                        case powersync::UpdateType::PATCH:
                            entry["op"] = "PATCH";
                            break;
                        case powersync::UpdateType::DELETE:
                            entry["op"] = "DELETE";
                            break;
                    }

                    entry["table"] = item.table;
                    entry["id"] = item.id;
                    if (item.data.has_value()) {
                        entry["data"] = json::parse(item.data.value());
                    }
                    entries.push_back(entry);
                }

                const auto curl = curl_easy_init();
                std::string response;
                const auto headers = curl_slist_append(nullptr, "Content-Type: application/json");

                auto request_body = json::object({"batch", entries});
                auto serialized_body = request_body.dump();

                curl_easy_setopt(curl, CURLOPT_URL, "http://localhost:6060/api/data");
                curl_easy_setopt(curl, CURLOPT_POST, 1);
                curl_easy_setopt(curl, CURLOPT_POSTFIELDS, serialized_body.c_str());
                curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
                curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
                curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

                if (auto res = curl_easy_perform(curl); res != CURLE_OK) {
                    completion.complete_error(res, "CURL request failed");
                    return;
                }

                long code;
                curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &code);
                if (code != 200) {
                    completion.complete_error(static_cast<int>(code), "Unexpected response code, body was: " + response);
                }

                curl_easy_cleanup(curl);
                curl_slist_free_all(headers);
                tx.complete();
            }

            std::cout << "Done with transactions iteration"  << std::endl;
            completion.complete_ok(std::monostate());
        }).detach();
    }

    ~DemoConnector() override = default;
};

int main() {
    using namespace powersync;
    set_logger(LogLevel::Info, [](LogLevel _, const char* message) {
        std::cout << message << std::endl;
    });

    Schema schema{};
    schema.tables.emplace_back(Table{"todos", {
        Column::text("description"),
        Column::integer("completed"),
        Column::text("list_id"),
    }});
    schema.tables.emplace_back(Table{"lists", {
        Column::text("name"),
    }});

    auto db = std::make_shared<Database>(std::move(Database::in_memory(schema)));
    db->spawn_sync_thread();

    auto subscription = SyncStream(*db, "lists").subscribe();

    auto status_watcher = db->watch_sync_status([db, subscription]() {
        const auto status = db->sync_status();
        std::cout << "Sync status: " << status << std::endl;

        const auto stream_status = status.for_stream(subscription.stream);
        if (stream_status.has_value()) {
            const auto progress = stream_status->progress;;
            std::cout << "Download progress: Has synced: " << stream_status->has_synced;
            if (progress.has_value()) {
                std::cout << ", progress: " << progress->downloaded << " / " << progress->total << std::endl;
            }
        }
    });

    auto connector = std::make_shared<DemoConnector>(db);
    db->connect(connector);

    auto watcher = db->watch_tables({"lists"}, [db] {
        std::cout << "Saw change on lists table" << std::endl;
        auto reader = db->reader();
        sqlite3_stmt *stmt = nullptr;
        check_rc(sqlite3_prepare_v2(reader, "SELECT id, name FROM lists", -1, &stmt, nullptr));

        while (sqlite3_step(stmt) == SQLITE_ROW) {
            std::cout << sqlite3_column_text(stmt, 0) << ": " << sqlite3_column_text(stmt, 1) << std::endl;
        }
        sqlite3_finalize(stmt);
    });

    for (std::string line; std::getline(std::cin, line);) {
        // TODO: Handle adding lists
        auto writer = db->writer();
        sqlite3_stmt *stmt;

        check_rc(sqlite3_prepare_v3(writer, "INSERT INTO lists (id, name) VALUES (uuid(), ?)", -1, 0, &stmt, nullptr));
        check_rc(sqlite3_bind_text(stmt, 1, line.c_str(), static_cast<int>(line.length()), SQLITE_TRANSIENT));

        const auto rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE) {
            throw std::runtime_error("SQLite error: " + std::string(sqlite3_errstr(rc)));
        }
    }
}
