#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) {
  _max_chunk_size = chunk_size;
  _chunks.push_back(std::make_shared<Chunk>());
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(this->is_empty(), "Columns may only be added to an empty table.");
  auto search = _column_name_mapping.find(name);
  DebugAssert(search == _column_name_mapping.end(), "Column names may not be duplicated");
  _column_name_mapping.insert({ name, (ColumnID) _column_names.size() });
  _column_names.push_back(name);
  _column_types.push_back(type);
  auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
  _chunks.front()->add_segment(segment);
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Check if the latest Chunk has already reached maximum size and create a new one if necessary
  if (this->is_new_chunk_needed()) {
    _chunks.push_back(std::make_shared<Chunk>());

    for (auto type_name : _column_types) {
      auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type_name);
      _chunks.back()->add_segment(segment);
    }
  }

  _chunks.back()->append(values);
}

bool Table::is_new_chunk_needed() {
  return _chunks.back()->size() >= _max_chunk_size;
}

uint16_t Table::column_count() const {
  return _column_names.size();
}

uint64_t Table::row_count() const {
  // Since we cannot delete data, all old chunks will have maximum chunk size
  return ((_chunks.size() - 1) * _max_chunk_size) + _chunks.back()->size();
}

ChunkID Table::chunk_count() const {
  return (ChunkID) _chunks.size();
}

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto search = _column_name_mapping.find(column_name);
  DebugAssert(search != _column_name_mapping.end(), "Could not find column_name");
  return search->second;
}

uint32_t Table::max_chunk_size() const {
  return _max_chunk_size;
}

bool Table::is_empty() {
  return _chunks.size() == 1 && _chunks.front()->size() == 0;
}

const std::vector<std::string>& Table::column_names() const {
  return _column_names;
}

const std::string& Table::column_name(ColumnID column_id) const {
  return _column_names.at(column_id);
}

const std::string& Table::column_type(ColumnID column_id) const {
  return _column_types.at(column_id);
}

Chunk& Table::get_chunk(ChunkID chunk_id) {
  return *_chunks.at(chunk_id);
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  return *_chunks.at(chunk_id);
}

}  // namespace opossum
