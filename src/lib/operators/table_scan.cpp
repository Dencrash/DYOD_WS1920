#pragma once

#include <vector>

#include "table_scan.hpp"
#include "types.hpp"


std::shared_ptr<const Table> Tablescan::on_execute() {
    auto table = _input_table_left();
    auto data_type = table->column_type(_column_id);
    auto scan_implementation = make_shared_by_data_type<AbstractOperator, BaseTableScanImpl>(data_type);
    auto chunk_count = table->chunk_count();
    auto column_count = table->column_count();
    auto full_position_list = make_shared<PosList>();


    for (chunk_index = 0; chunk_index < chunk_count; ++chunk_index) {
        auto position_list = scan_implementation.scan_chunk(table->get_chunk(chunk_index), chunk_index);
        full_position_list->insert(full_position_list.end(), position_list.begin(), position_list.end());
    }

    Chunk new_chunk;

    for (column_index = 0; column_index < column_count; ++column_index) {
        // Remember to change referenced table in case of reference segment
        new_chunk.add_segment(ReferenceSegment(table, column_index, full_position_list));
    }
    auto new_table = make_shared<Table>(); 
    new_table->emplace_chunk(new_chunk);
    return new_table;
}
