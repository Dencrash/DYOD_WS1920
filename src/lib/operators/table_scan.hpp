#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "storage/reference_segment.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value) : AbstractOperator(in, in), _column_id(column_id), _scan_type(scan_type), _search_value(search_value) {}

  ~TableScan();

  ColumnID column_id() const { return _column_id; }

  ScanType scan_type() const { return _scan_type; }

  const AllTypeVariant& search_value() const { return _search_value; }

 protected:
  std::shared_ptr<const Table> _on_execute() override;
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  template <typename T>
  class BaseTableScanImpl : public AbstractOperator {
   public:
    BaseTableScanImpl(AllTypeVariant search_value): _search_value(type_cast<T>(search_value)) {};

    const T& search_value() const { return type_cast<T>(_search_value); }

   protected:
    T _search_value;

    std::shared_ptr<const PosList> BaseTableScanImpl::_scan_chunk(Chunk chunk, ChunkID chunk_id) {
      if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(chunk.get_segment(_column_id))) {
        auto compare_function = _create_compare_function();
        return _scan_value_segment(value_segment, chunk_id, compare_function);
      } else if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(_column_id))) {
          return _scan_reference_segment(reference_segment, chunk_id);
      } else if (auto dictionary_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(_column_id))) {
          return _scan_dictionary_segment(dictionary_segment, chunk_id);
      } else {
          throw std::runtime_error("Segment Type does not match search type");
      }
    }

    std::shared_ptr<const PosList> _scan_dictionary_segment(shared_ptr<DictionarySegment<T>> segment, ChunkID chunk_id) {
      PosList position_list;
      return make_shared<const PosList>(position_list);
    }

    std::shared_ptr<const PosList> _scan_reference_segment(shared_ptr<ReferenceSegment> segment, ChunkID chunk_id) {
      PosList position_list;
      return make_shared<const  PosList>(position_list);
    }

    std::shared_ptr<const PosList> _scan_value_segment(shared_ptr<ValueSegment<T>> segment, ChunkID chunk_id, std::function<bool(const T&)> comparator) {
      PosList position_list;
      auto values = segment->values();
      auto values_size = values.size();
      for (value_index = 0; value_index < values_size; ++value_index) {
        if (comparator(values[value_index])) {
          position_list.push_back(RowID{chunk_id, value_index});
        }
      }

      return make_shared<const PosList>(position_list);
    }

    std::function<bool(const T&)> _create_compare_function() {
      switch (_scan_type) {
        case ScanType::OpEquals:
          return [=](const T& value) -> bool { return value == _search_value; };
        case ScanType::OpLessThanEquals:
          return [=](const T& value) -> bool { return value <= _search_value; };
        case ScanType::OpGreaterThanEquals:
          return [=](const T& value) -> bool { return value >= _search_values; };
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
