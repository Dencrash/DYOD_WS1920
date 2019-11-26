#pragma once

#include <memory>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * Operator that wraps a table.
 */
class TableWrapper : public AbstractOperator {
 public:
  // create a TableWrapper operator
  // table gives the specific table that will be used as output
  explicit TableWrapper(const std::shared_ptr<const Table> table);

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  // Table to retrieve
  const std::shared_ptr<const Table> _table;
};
}  // namespace opossum
