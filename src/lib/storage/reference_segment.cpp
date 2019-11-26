#include "reference_segment.hpp"

namespace opossum {

ReferenceSegment::ReferenceSegment(const std::shared_ptr<const Table> referenced_table,
                                    const ColumnID referenced_column_id,
                                    const std::shared_ptr<const PosList> pos) : _referenced_table(referenced_table),
                                      _position_list(pos), _referenced_column(referenced_column_id) {}

AllTypeVariant ReferenceSegment::operator[](const ChunkOffset chunk_offset) const {
  const RowID target_id = _position_list->at(chunk_offset);

  auto target_segment = _referenced_table->get_chunk(target_id.chunk_id).get_segment(_referenced_column);

  return (*target_segment)[target_id.chunk_offset];
}

size_t ReferenceSegment::size() const {
  return _position_list->size();
}

const std::shared_ptr<const PosList> ReferenceSegment::pos_list() const {
  return _position_list;
}

const std::shared_ptr<const Table> ReferenceSegment::referenced_table() const {
  return _referenced_table;
}

ColumnID ReferenceSegment::referenced_column_id() const {
  return _referenced_column;
}

size_t ReferenceSegment::estimate_memory_usage() const {
  // Make sure that we can access a relevant value to get the correct type
  if (!(_position_list->size())) {
    return 0;
  }
  auto first_referenced_chunk = (*_position_list)[0].chunk_id;
  auto relevant_segment = _referenced_table->get_chunk(first_referenced_chunk).get_segment(_referenced_column);
  return _position_list->size() * relevant_segment->estimate_memory_usage_per_element();
}

size_t ReferenceSegment::estimate_memory_usage_per_element() const {
  auto relevant_segment = _referenced_table->get_chunk(ChunkID{0}).get_segment(_referenced_column);
  return relevant_segment->estimate_memory_usage_per_element();
}

} // namespace opossum




