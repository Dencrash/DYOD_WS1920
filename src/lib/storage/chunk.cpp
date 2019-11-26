#include <iomanip>
#include <iterator>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "chunk.hpp"

#include "utils/assert.hpp"

namespace opossum {

void Chunk::add_segment(std::shared_ptr<BaseSegment> segment) { _chunk_columns.push_back(segment); }

void Chunk::append(const std::vector<AllTypeVariant>& values) {
  DebugAssert(values.size() == _chunk_columns.size(), "Size of values and segments need to match.");
  for (size_t column_index = 0; column_index < _chunk_columns.size(); ++column_index) {
    _chunk_columns.at(column_index)->append(values.at(column_index));
  }
}

std::shared_ptr<BaseSegment> Chunk::get_segment(ColumnID column_id) const { return _chunk_columns.at(column_id); }

uint16_t Chunk::column_count() const { return _chunk_columns.size(); }

uint32_t Chunk::size() const {
  if (!_chunk_columns.empty()) {
    return _chunk_columns.front()->size();
  }

  // Fallback in case no ValueSegment has been added yet.
  return 0;
}

}  // namespace opossum
