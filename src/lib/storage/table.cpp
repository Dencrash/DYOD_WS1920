#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) : _max_chunk_size(chunk_size){
  _chunks.push_back(std::make_shared<Chunk>());
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  auto search = _column_name_mapping.find(name);
  DebugAssert(search == _column_name_mapping.end(), "Column names may not be duplicated");
  _column_name_mapping.insert({name, (ColumnID)_column_names.size()});
  _column_names.push_back(name);
  _column_types.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  DebugAssert(this->is_empty(), "Columns may only be added to an empty table.");
  add_column_definition(name, type);
  auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type);
  _chunks.front()->add_segment(segment);
}

void Table::append(std::vector<AllTypeVariant> values) {
  // Check if the latest Chunk has already reached maximum size and create a new one if necessary
  if (this->is_new_chunk_needed()) {
    create_new_chunk();
  }
  _chunks.back()->append(values);
}

void Table::create_new_chunk() {
  _chunks.push_back(std::make_shared<Chunk>());
  for (auto type_name : _column_types) {
    auto segment = make_shared_by_data_type<BaseSegment, ValueSegment>(type_name);
    _chunks.back()->add_segment(segment);
  }
}

bool Table::is_new_chunk_needed() { return _chunks.back()->size() >= _max_chunk_size; }

uint16_t Table::column_count() const { return _column_names.size(); }

uint64_t Table::row_count() const {
  // Since we cannot delete data, all old chunks will have maximum chunk size
  return ((_chunks.size() - 1) * _max_chunk_size) + _chunks.back()->size();
}

ChunkID Table::chunk_count() const { return (ChunkID)_chunks.size(); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto search = _column_name_mapping.find(column_name);
  DebugAssert(search != _column_name_mapping.end(), "Could not find column_name");
  return search->second;
}

uint32_t Table::max_chunk_size() const { return _max_chunk_size; }

bool Table::is_empty() { return _chunks.size() == 1 && _chunks.front()->size() == 0; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names.at(column_id); }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types.at(column_id); }

Chunk& Table::get_chunk(ChunkID chunk_id) { return *_chunks.at(chunk_id); }

const Chunk& Table::get_chunk(ChunkID chunk_id) const { return *_chunks.at(chunk_id); }

void Table::compress_chunk(ChunkID chunk_id) {
  DebugAssert(chunk_id < _chunks.size(), "ChunkID out of range");
  const auto chunk = _chunks[chunk_id];

  // create new empty chunk  
  auto compressed_chunk = std::make_shared<Chunk>();

  // number of threads needed
  auto n_columns = chunk->column_count();
  std::vector<std::thread> threads;
  threads.reserve(n_columns);
  std::vector<std::shared_ptr<BaseSegment>> dictionary_segments(n_columns);

  for (size_t segment_index = 0; segment_index < dictionary_segments.size(); ++segment_index) {
    auto segment = chunk->get_segment(static_cast<ColumnID>(segment_index));
    auto segment_type = column_type(static_cast<ColumnID>(segment_index));
    threads.emplace_back(std::thread([&dictionary_segments] (size_t segment_index, auto segment, auto type) {
        auto compressed_chunk = make_shared_by_data_type<BaseSegment, DictionarySegment>
                        (type, segment);

        dictionary_segments[segment_index] = compressed_chunk;

    }, segment_index, segment, segment_type));
  }

  for (size_t thread_index = 0; thread_index < threads.size(); ++thread_index) {
    threads[thread_index].join();
  }

  for (int segment = 0; segment < n_columns; ++segment) {
    compressed_chunk->add_segment(dictionary_segments.at(segment));
  }

  _chunks[chunk_id] = std::move(compressed_chunk);
}

void Table::emplace_chunk(Chunk chunk) {
  if (_chunks.front()->size()) {
    _chunks.emplace_back(std::make_shared<Chunk>());
  }
  else {
    auto chunk_ptr = std::make_shared<Chunk>(std::move(chunk));
    _chunks.front() = std::move(chunk_ptr);
  }
}

}  // namespace opossum
