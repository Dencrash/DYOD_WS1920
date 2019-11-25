#include <vector>
#include <memory>

#include "table_scan.hpp"
#include "types.hpp"
#include "resolve_type.hpp"

namespace opossum {

std::shared_ptr<const Table> TableScan::_on_execute() {
    auto table = _input_table_left();
    auto referenced_table = table;
    if (table->column_count() == 0) {
        return std::make_shared<const Table>();
    }
    auto data_type = table->column_type(_column_id);
    auto scan_implementation = make_shared_by_data_type<AbstractOperator, BaseTableScanImpl>(data_type, _input_left, _scan_type, _column_id, _search_value);

    scan_implementation->execute();
    return scan_implementation->get_output();
}

} // namespace opossum
