#pragma once

#include <string>
#include "constants.hpp"
#include <spdlog/spdlog.h>
#include "external/csv2/csv2.hpp"
#include <filesystem>
#include <vector>
#include <unordered_map>
#include <unordered_set>

class CarSalesProcessor
{
public:
    CarSalesProcessor();
    ~CarSalesProcessor();
    ErrorCode readCsv(std::string csv_path);
    std::vector<std::unordered_map<std::string, std::string>> filterRowsByColumnValues(
        std::unordered_map<std::string, std::unordered_set<std::string>> column_values,
        std::unordered_set<std::string> rows_to_store,
        int worker_threads = 1
    );
    std::pair<ErrorCode, GroupedSumList> groupAndSumColumns (
        const std::vector<std::unordered_map<std::string, std::string>>& rows,
        const std::string& group_by_column,
        const std::vector<std::string>& columns_to_sum,
        int worker_threads = 1,
        bool sorted = false,
        std::string sort_by_col = ""
    );

private:
    std::string csv_file_path_;
    int total_rows_ = 0;
    int total_cols_ = 0;
    csv2::Reader<> csv_reader_;
    std::vector<std::string> headers_;
    std::vector<std::unordered_map<std::string, std::string>> filterRowsByColumnValuesWorker (
        std::unordered_map<std::string, std::unordered_set<std::string>> column_values,
        std::unordered_set<std::string> rows_to_store,
        int row_start, 
        int row_end
    );
    void groupAndSumColumnsWorker(
        const std::vector<std::unordered_map<std::string, std::string>>& rows,
        size_t start_idx,
        size_t end_idx,
        const std::string& group_by_column,
        const std::vector<std::string>& columns_to_sum,
        std::unordered_map<std::string, std::unordered_map<std::string, unsigned long long>>& local_result,
        std::mutex& log_mtx
    );
    bool wildCardMatch(const std::string& text, const std::string& pattern);
};