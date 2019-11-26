#include <memory>
#include <vector>

#include "resolve_type.hpp"
#include "table_scan.hpp"
#include "types.hpp"

namespace opossum {

std::shared_ptr<const Table> TableScan::_on_execute() {
  auto table = _input_table_left();
  // If the table has no columns make sure that we never try to access unavailable segments and return empty table
  if (table->column_count() == 0) {
    return std::make_shared<const Table>();
  }
  auto data_type = table->column_type(_column_id);
  // Create a TableScanImpl operator with the correct type template of the target column
  auto scan_implementation = make_shared_by_data_type<AbstractOperator, BaseTableScanImpl>(
      data_type, _input_left, _scan_type, _column_id, _search_value);

  // Execute the internal operator and return it's output as result
  scan_implementation->execute();
  return scan_implementation->get_output();
}

}  // namespace opossum
