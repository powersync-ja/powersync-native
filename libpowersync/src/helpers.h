#pragma once

#include <vector>

namespace powersync {

struct RustTableHelper {
    std::vector<std::vector<internal::Column>> columns = {};
    std::vector<internal::Table> tables = {};

    static void map_column(std::vector<internal::Column>& target, const Column& column);
    void map_table(const Table& table);

    internal::RawSchema to_rust();
};

}
