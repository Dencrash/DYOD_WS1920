#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base_segment.hpp"
#include "dictionary_segment.hpp"
#include "table.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "value_segment.hpp"

namespace opossum {

// ReferenceSegment is a specific segment type that stores all its values as position list of a referenced segment
class ReferenceSegment : public BaseSegment {
 public:
  // creates a reference segment
  // Parameters:
  // referenced_table speficies the specific table that will be referenced
  // referenced_column_id specifies the column that this segment is referencing on the table
  // pos specifies all rows that will be part of the ReferenceSegment
  // note: A table will only have one ReferenceSegment per Column
  ReferenceSegment(const std::shared_ptr<const Table> referenced_table, const ColumnID referenced_column_id,
                   const std::shared_ptr<const PosList> pos);

  // return the value at chunk_offset from this ReferenceSegment
  // note: this does not return this chunk_offset at the referenced_table
  AllTypeVariant operator[](const ChunkOffset chunk_offset) const override;

  // Override append operator since ReferenceSegments are immutable
  void append(const AllTypeVariant&) override { throw std::logic_error("ReferenceSegment is immutable"); };

  // return number of entries in the ReferenceSegment
  // note: this can be more than the maximum chunk size of a table
  size_t size() const override;

  // return a reference to the underlying position list
  const std::shared_ptr<const PosList> pos_list() const;

  // return a reference to the referenced_table
  const std::shared_ptr<const Table> referenced_table() const;

  // return the referenced_column_id
  ColumnID referenced_column_id() const;

  // return an estimation for the memory usage of all entries in this ReferenceSegment
  // note: does not only return the size of the pointers but of a materialized table
  size_t estimate_memory_usage() const override;

  // return the estimated memory usage for a single element
  // note: will return the per element estimation from the referenced table
  size_t estimate_memory_usage_per_element() const override;

 protected:
  const std::shared_ptr<const Table> _referenced_table;
  const std::shared_ptr<const PosList> _position_list;

  const ColumnID _referenced_column;
};

}  // namespace opossum
