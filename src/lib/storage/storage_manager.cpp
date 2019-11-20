#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) {
  auto insert_result = _tables.insert({ name, table });
  DebugAssert(insert_result.second, "Tablename already exists.");
}

void StorageManager::drop_table(const std::string& name) {
  auto search = _tables.find(name);
  DebugAssert(search != _tables.end(), "Tablename could not be found.");
  _tables.erase(name);
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const {
  auto search = _tables.find(name);
  DebugAssert(search != _tables.end(), "Tablename could not be found.");
  return search->second;
}

bool StorageManager::has_table(const std::string& name) const {
  auto search = _tables.find(name);
  return search != _tables.end();
}

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  names.reserve(_tables.size());
  for (auto pair : _tables) {
    names.emplace_back(pair.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (auto pair : _tables) {
    out << pair.first << " | " << pair.second->column_count() << " | "
        << pair.second->row_count() << " | " << pair.second->chunk_count();
  }
}

void StorageManager::reset() {
  _tables.clear();
}

}  // namespace opossum
