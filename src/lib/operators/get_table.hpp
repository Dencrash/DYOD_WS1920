#pragma once

#include <memory>
#include <string>
#include <vector>

#include "../storage/storage_manager.hpp"
#include "abstract_operator.hpp"

namespace opossum {

// operator to retrieve a table from the StorageManager by specifying its name
class GetTable : public AbstractOperator {
 public:
  // Instanz of singleton object StorageManager to get one specific table by requested tablename
  StorageManager& storage = StorageManager::get();

  explicit GetTable(const std::string& name) {
    this->_table_name = name;
    _on_execute();
  }

  const std::string& table_name() const { return _table_name; }

 protected:
  std::shared_ptr<const Table> _on_execute() override {
    bool table_exist = storage.has_table(_table_name);
    //DebugAssert(!table_exist, "The executed tablename does not exist.");
    if (table_exist) {
      return storage.get_table(_table_name);
    }
    return nullptr;
  }

  std::string _table_name;
};
}  // namespace opossum
