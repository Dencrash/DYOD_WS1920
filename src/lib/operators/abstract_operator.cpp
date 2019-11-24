#include "abstract_operator.hpp"

#include <chrono>
#include <memory>
#include <string>
#include <vector>

#include "storage/table.hpp"
#include "utils/assert.hpp"

namespace opossum {

AbstractOperator::AbstractOperator(const std::shared_ptr<const AbstractOperator> left,
                                   const std::shared_ptr<const AbstractOperator> right)
    : _input_left(left), _input_right(right) {}

void AbstractOperator::execute() { _output = _on_execute(); }

std::shared_ptr<const Table> AbstractOperator::get_output() const {
  if (!_output) {
    throw std::runtime_error("Operator was never executed");
  }

  return _output;
}

std::shared_ptr<const Table> AbstractOperator::_input_table_left() const { return _input_left->get_output(); }

std::shared_ptr<const Table> AbstractOperator::_input_table_right() const { return _input_right->get_output(); }

}  // namespace opossum
