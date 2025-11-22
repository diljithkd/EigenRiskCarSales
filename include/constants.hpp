#pragma once

#include <string>
#include <utility>
#include <unordered_map>
#include <vector>

enum class ErrorCode {
  kOk = 0,
  kFileNotFound = 1,
  kFileReadError = 2,
  kInvalidCharacter = 3,
  kInvalidData = 4,
  kUnknownError = 100
};

using GroupedSumEntry = std::pair<
    std::string,
    std::unordered_map<std::string, unsigned long long>
>;

using GroupedSumList = std::vector<GroupedSumEntry>;
