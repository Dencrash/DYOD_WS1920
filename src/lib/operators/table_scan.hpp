#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/reference_segment.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  // Create a TableScan operator
  // Parameters:
  // 'in' gives us the table or operator that will be used as function input
  // column_id specifies the relevant column
  // search_value and scan_type specify the compare function
  // that will be executed to decide, if a value is part of the result
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value)
      : AbstractOperator(in, in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

  // return the column_id of the column that our comparison will check against
  ColumnID column_id() const { return _column_id; }

  // return the type of scan we will execute (e.g., OpEquals(==), OpLessThan(<), etc.)
  ScanType scan_type() const { return _scan_type; }

  // return the value that our table elements will be compared against
  const AllTypeVariant& search_value() const { return _search_value; }

 protected:
  // implementation of this operator to give the table that will be set as output
  // note: this will call the internal implementation of BaseTableScanImpl
  std::shared_ptr<const Table> _on_execute() override;

  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  /*
  ** Internal class that implements the TableScan with the relevant type
  */
  template <typename T>
  class BaseTableScanImpl : public AbstractOperator {
   public:
    // Create a BaseTableScanImpl operator
    // Parameters are equal to the TableScan operator at the top of the file
    BaseTableScanImpl(const std::shared_ptr<const AbstractOperator> in, ScanType scan_type, ColumnID column_id,
                      AllTypeVariant search_value)
        : AbstractOperator(in, in),
          _scan_type(scan_type),
          _column_id(column_id),
          _search_value(type_cast<T>(search_value)) {}

   protected:
    const ScanType _scan_type;
    const ColumnID _column_id;
    const T _search_value;
    std::function<bool(const T&)> _compare_function;

    std::shared_ptr<const Table> _on_execute() {
      _compare_function = _create_compare_function();
      auto table = _input_table_left();
      auto referenced_table = table;
      auto chunk_count = table->chunk_count();
      auto column_count = table->column_count();
      auto full_position_list = std::make_shared<PosList>();

      for (ChunkID chunk_id{0}; chunk_id < chunk_count; ++chunk_id) {
        auto& chunk = table->get_chunk(chunk_id);
        if (!chunk.size()) {
          continue;
        }
        auto position_list = _scan_chunk(chunk, chunk_id);
        full_position_list->insert(full_position_list->end(), position_list->begin(), position_list->end());
      }

      Chunk new_chunk;

      if (auto reference_segment =
              std::dynamic_pointer_cast<ReferenceSegment>(table->get_chunk(ChunkID{0}).get_segment(_column_id))) {
        referenced_table = reference_segment->referenced_table();
      }

      for (auto column_index = 0; column_index < column_count; ++column_index) {
        // Remember to change referenced table in case of reference segment
        new_chunk.add_segment(std::make_shared<ReferenceSegment>(
            ReferenceSegment(referenced_table, (ColumnID)column_index, full_position_list)));
      }

      auto new_table = std::make_shared<Table>();
      new_table->emplace_chunk(std::move(new_chunk));
      for (auto column_index = 0; column_index < column_count; ++column_index) {
        new_table->add_column_definition(table->column_name(ColumnID{column_index}),
                                         table->column_type(ColumnID{column_index}));
      }
      return new_table;
    }

    std::shared_ptr<const PosList> _scan_chunk(const Chunk& chunk, ChunkID chunk_id) {
      if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(chunk.get_segment(_column_id))) {
        return _scan_value_segment(value_segment, chunk_id);
      } else if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(_column_id))) {
        return _scan_reference_segment(reference_segment, chunk_id);
      } else if (auto dictionary_segment =
                     std::dynamic_pointer_cast<DictionarySegment<T>>(chunk.get_segment(_column_id))) {
        return _scan_dictionary_segment(dictionary_segment, chunk_id);
      } else {
        throw std::runtime_error("Segment Type does not match search type");
      }
    }

    std::shared_ptr<const PosList> _scan_dictionary(std::shared_ptr<DictionarySegment<T>> segment, ChunkID chunk_id) {
      PosList position_list;
      auto segment_size = segment->size();
      for (size_t row_index = 0; row_index < segment_size; ++row_index) {
        if (_compare_function(segment->get((ChunkOffset)row_index))) {
          position_list.push_back(RowID{chunk_id, (ChunkOffset)row_index});
        }
      }
      return std::make_shared<const PosList>(position_list);
    }

    std::shared_ptr<const PosList> _scan_dictionary_segment(std::shared_ptr<DictionarySegment<T>> segment,
                                                            ChunkID chunk_id) {
      PosList position_list;

      auto dictionary = segment->dictionary();
      auto attribute_vector = segment->attribute_vector();

      for (size_t row_index = 0; row_index < segment->size(); ++row_index) {
        auto value = segment->value_by_value_id(static_cast<ValueID>(row_index));
        ValueID search_value_id = segment->lower_bound(value);
        position_list.push_back(RowID{chunk_id, search_value_id});
      }
      return std::make_shared<const PosList>(position_list);
    }

    std::shared_ptr<const PosList> _scan_reference_segment(std::shared_ptr<ReferenceSegment> segment,
                                                           ChunkID chunk_id) {
      PosList position_list;
      auto referenced_table = segment->referenced_table();
      auto referenced_position_list = segment->pos_list();
      ChunkID current_chunk_id = referenced_table->chunk_count();
      std::shared_ptr<ValueSegment<T>> value_segment;
      std::shared_ptr<DictionarySegment<T>> dictionary_segment;
      bool is_value_segment = false;

      for (const auto& row_id : *referenced_position_list) {
        if (current_chunk_id != row_id.chunk_id) {
          if ((value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(
                   referenced_table->get_chunk(row_id.chunk_id).get_segment(_column_id)))) {
            is_value_segment = true;
          } else if ((dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(
                          referenced_table->get_chunk(row_id.chunk_id).get_segment(_column_id)))) {
            is_value_segment = false;
          } else {
            throw std::runtime_error("Segment Type does not match search type");
          }
        }

        if (is_value_segment) {
          if (_compare_function(type_cast<T>((*value_segment)[row_id.chunk_offset]))) {
            position_list.push_back(RowID{row_id.chunk_id, row_id.chunk_offset});
          }
        } else {
          if (_compare_function(dictionary_segment->get(row_id.chunk_offset))) {
            position_list.push_back(RowID{row_id.chunk_id, row_id.chunk_offset});
          }
        }
      }

      return std::make_shared<const PosList>(position_list);
    }

    std::shared_ptr<const PosList> _scan_value_segment(std::shared_ptr<ValueSegment<T>> segment, ChunkID chunk_id) {
      PosList position_list;
      auto values = std::make_shared<std::vector<T>>(segment->values());
      auto values_size = values->size();
      for (uint32_t value_index = 0; value_index < values_size; ++value_index) {
        if (_compare_function((*values)[value_index])) {
          position_list.push_back(RowID{chunk_id, value_index});
        }
      }

      return std::make_shared<const PosList>(position_list);
    }

    std::function<bool(const T&)> _create_compare_function() {
      switch (_scan_type) {
        case ScanType::OpEquals:
          return [=](const T& value) -> bool { return value == _search_value; };
        case ScanType::OpLessThanEquals:
          return [=](const T& value) -> bool { return value <= _search_value; };
        case ScanType::OpGreaterThanEquals:
          return [=](const T& value) -> bool { return value >= _search_value; };
        case ScanType::OpLessThan:
          return [=](const T& value) -> bool { return value < _search_value; };
        case ScanType::OpGreaterThan:
          return [=](const T& value) -> bool { return value > _search_value; };
        case ScanType::OpNotEquals:
          return [=](const T& value) -> bool { return value != _search_value; };
        default:
          throw std::runtime_error("Unknow ScanType");
      }
    }
  };
};

}  // namespace opossum
