#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl;
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value) {
    // //this->input_right();
    // this->_column_id = column_id;
    // this->_scan_type = scan_type;
    // this->_search_value = search_value;
  }

  ~TableScan();

  ColumnID column_id() const { return _column_id; }

  ScanType scan_type() const { return _scan_type; }

  const AllTypeVariant& search_value() const { return _search_value; }

  // void make_unique_by_column_type(std::shared_ptr<BaseSegment> base_segment) const {
  //   auto pointer_cast = std::dynamic_pointer_cast<BaseSegment>(base_segment);
  // }

 protected:
  const ColumnID _column_id;
  const ScanType _scan_type;
  const AllTypeVariant _search_value;

  std::shared_ptr<const Table> _on_execute() override;

  // Nested class declaration
  template <typename T>
  class BaseTableScanImpl : public AbstractOperator {
   public:
    void access_table_scan(TableScan* t);

    //const T& search_value() const { return type_cast<T>(_search_value); }

    //  protected:
    //   AllTypeVariant _search_value;
  };
};

}  // namespace opossum
