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

  // creates a GetTable object
  // the name parameter specifies the specific table_name that is going to be requested from the StorageManager
  explicit GetTable(const std::string& name) : _table_name(name) {}

  // return the tablename that the operator is looking for
  const std::string& table_name() const { return _table_name; }

 protected:
  // return the relevant table from the storage manager.
  // This function will throw an unknown table error, if the tablename is not known.
  std::shared_ptr<const Table> _on_execute() override { return storage.get_table(_table_name); }

  const std::string _table_name;
};

}  // namespace opossum
