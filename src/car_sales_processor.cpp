#include "car_sales_processor.hpp"

CarSalesProcessor::CarSalesProcessor()
{

}

CarSalesProcessor::~CarSalesProcessor()
{

}

void CarSalesProcessor::groupAndSumColumnsWorker(
    const std::vector<std::unordered_map<std::string, std::string>>& rows,
    size_t start_idx,
    size_t end_idx,
    const std::string& group_by_column,
    const std::vector<std::string>& columns_to_sum,
    std::unordered_map<std::string, std::unordered_map<std::string, unsigned long long>>& local_result,
    std::mutex& log_mtx)
{
    for (size_t i = start_idx; i < end_idx; ++i) {
        const auto& row = rows[i];
        auto grp_it = row.find(group_by_column);
        if (grp_it == row.end()) {
            std::lock_guard<std::mutex> lock(log_mtx);
            spdlog::error("Missing group by column: {}", group_by_column);
            local_result.clear();
            return;
        }

        const std::string& group_value = grp_it->second;
        auto& inner = local_result[group_value];

        for (const auto& col : columns_to_sum) {
            if (inner.find(col) == inner.end()) inner[col] = 0;

            auto col_it = row.find(col);
            if (col_it == row.end()) {
                std::lock_guard<std::mutex> lock(log_mtx);
                spdlog::error("Missing summation column: {}", col);
                local_result.clear();
                return;
            }

            try {
                unsigned long long val = std::stoull(col_it->second);
                inner[col] += val;
            } catch (...) {
                std::lock_guard<std::mutex> lock(log_mtx);
                spdlog::error("Invalid numeric value in column {} : {}", col, col_it->second);
            }
        }
    }
}


std::pair<ErrorCode, GroupedSumList>
CarSalesProcessor::groupAndSumColumns(
    const std::vector<std::unordered_map<std::string, std::string>>& rows,
    const std::string& group_by_column,
    const std::vector<std::string>& columns_to_sum,
    int worker_threads,
    bool sorted,
    std::string sort_by_col)
{
    if (worker_threads <= 0) worker_threads = 1;

    std::vector<std::unordered_map<std::string, std::unordered_map<std::string, unsigned long long>>> partial_results(worker_threads);
    std::vector<std::thread> threads;
    std::mutex log_mtx;

    size_t chunk_size = (rows.size() + worker_threads - 1) / worker_threads;

    for (int t = 0; t < worker_threads; ++t) {
        size_t start_idx = t * chunk_size;
        size_t end_idx = std::min(start_idx + chunk_size, rows.size());
        threads.emplace_back(&CarSalesProcessor::groupAndSumColumnsWorker, this,
                             std::cref(rows), start_idx, end_idx,
                             std::cref(group_by_column), std::cref(columns_to_sum),
                             std::ref(partial_results[t]), std::ref(log_mtx));
    }

    for (auto& th : threads) th.join();

    std::unordered_map<std::string, std::unordered_map<std::string, unsigned long long>> temp;
    for (const auto& partial : partial_results) {
        if (partial.size() == 0) {
            return {ErrorCode::kInvalidData, {}};
        }
        for (const auto& [group_value, sums] : partial) {
            auto& inner = temp[group_value];
            for (const auto& [col, val] : sums) {
                inner[col] += val;
            }
        }
    }

    GroupedSumList result;
    result.reserve(temp.size());
    for (auto& [group_value, sums] : temp) {
        result.emplace_back(group_value, std::move(sums));
    }

    if (sorted) {
        auto it = std::find(columns_to_sum.begin(), columns_to_sum.end(), sort_by_col);
        if (it == columns_to_sum.end()) {
            spdlog::warn("Column to sort by '{}' is not found. Returning unsorted data", sort_by_col);
        } else {
            std::sort(result.begin(), result.end(),
                      [&sort_by_col](const auto& a, const auto& b) {
                          return a.second.at(sort_by_col) > b.second.at(sort_by_col);
                      });
        }
    }

    return {ErrorCode::kOk, std::move(result)};
}




bool CarSalesProcessor::wildCardMatch(const std::string& text, const std::string& pattern) 
{
    size_t star_pos = pattern.find('*');

    if (star_pos == std::string::npos) {
        return text == pattern;
    }

    if (star_pos == 0) {
        std::string suffix = pattern.substr(1);
        if (suffix.empty()) return true;
        return text.size() >= suffix.size() &&
               text.compare(text.size() - suffix.size(), suffix.size(), suffix) == 0;
    }

    if (star_pos == pattern.size() - 1) {
        std::string prefix = pattern.substr(0, pattern.size() - 1);
        if (prefix.empty()) return true;
        return text.rfind(prefix, 0) == 0;
    }

    return false;
}


std::vector<std::unordered_map<std::string, std::string>> CarSalesProcessor::filterRowsByColumnValuesWorker(
    std::unordered_map<std::string, std::unordered_set<std::string>> column_values,
    std::unordered_set<std::string> rows_to_store,
    int row_start, 
    int row_end)
{
    std::vector<std::unordered_map<std::string, std::string>> res;
    int row_count = 0;
    for (auto row : csv_reader_) {
        row_count++;
        if (row_count < row_start) {
            continue;
        }
        if (row_count > row_end) {
            break;
        }
        int num_matches_found = 0;
        size_t col_idx = 0;
        std::unordered_map<std::string, std::string> temp_row;
        for (auto cell : row) {
            std::string val;
            cell.read_value(val);
            const std::string& col_name = headers_[col_idx];

            // store if present in rows_to_store
            if (rows_to_store.find(col_name) != rows_to_store.end()) {
                temp_row[col_name] = val;
            }
            // if header is present in columns to find
            auto col_header_name_it = column_values.find(col_name);
            if (col_header_name_it != column_values.end()) {
                auto col_value_it = column_values[col_name].find(val);
                // if the column value is actually present in the look up values of this header
                bool matched = false;
                const auto& patterns = column_values[col_name];

                for (const auto& pattern : patterns) {
                    if (wildCardMatch(val, pattern)) {
                        matched = true;
                        break;
                    }
                }

                if (matched) {
                    num_matches_found++;
                }
            }
            col_idx++;
        }
        if (num_matches_found == static_cast<int>(column_values.size())) {
            res.push_back(temp_row);
        }
    }
    return res;
}

std::vector<std::unordered_map<std::string, std::string>> CarSalesProcessor::filterRowsByColumnValues(
    std::unordered_map<std::string, std::unordered_set<std::string>> column_values,
    std::unordered_set<std::string> rows_to_store,
    int worker_threads)
{
    if (worker_threads <= 0) {
        worker_threads = 1;
    }

    std::vector<std::unordered_map<std::string, std::string>> final_result;
    final_result.reserve(total_rows_); 

    int chunk_size = (total_rows_ + worker_threads - 1) / worker_threads;

    std::vector<std::thread> threads;
    std::vector<std::vector<std::unordered_map<std::string, std::string>>> thread_results(worker_threads);

    for (int i = 0; i < worker_threads; i++) {
        int start = i * chunk_size;
        int end   = std::min(start + chunk_size, total_rows_);

        threads.emplace_back([&, i, start, end]() {
            thread_results[i] = filterRowsByColumnValuesWorker(
                column_values,
                rows_to_store,
                start,
                end
            );
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    for (auto& v : thread_results) {
        final_result.insert(final_result.end(),
                            std::make_move_iterator(v.begin()),
                            std::make_move_iterator(v.end()));
    }

    return final_result;
}

ErrorCode CarSalesProcessor::readCsv(std::string csv_path)
{
    csv_file_path_ = csv_path;
    if (!std::filesystem::exists(csv_file_path_)) {
        spdlog::error("CSV file not found : {}", csv_file_path_);
        return ErrorCode::kFileNotFound;
    }

    spdlog::info("Reading CSV File : {}", csv_file_path_); 
    if (!csv_reader_.mmap(csv_file_path_)) {
        spdlog::error("Failed to open CSV file : {}", csv_file_path_);
        return ErrorCode::kUnknownError;
    }

    auto header_row = csv_reader_.header();
    for (const auto& cell : header_row) {
        std::string h;
        cell.read_value(h);
        headers_.push_back(h);
    }
    total_rows_ = 0;
    for (auto row : csv_reader_) {
        total_rows_++;
    }

    total_cols_ = headers_.size();
    spdlog::info("CSV has {} rows", total_rows_); 
    spdlog::info("CSV has {} columns", total_cols_);    
    spdlog::info("CSV File Read Complete : {}", csv_file_path_); 
    return ErrorCode::kOk;
}