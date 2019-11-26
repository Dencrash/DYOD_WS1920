#include <iostream>
#include <memory>
#include <string>

#include "../lib/operators/get_table.hpp"
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/utils/assert.hpp"

int main() {
  opossum::Assert(true, "We can use opossum files here :)");
  std::cout << "+++++++++++++ Starting playground: +++++++++++++" << std::endl;

  // std::string table_name = "first_table";
  // // Create singleton
  // auto& sm = opossum::StorageManager::get();
  // // Create table with no entry and with one entry
  // std::shared_ptr<opossum::Table> t1 = std::make_shared<opossum::Table>(2);

  // // Add table to storage manager
  // sm.add_table(table_name, t1);

  // // Get table from StorageManager by tablename
  // bool hasTable = sm.has_table(table_name);

  // std::cout << "Table with given table_name exist= " << hasTable << std::endl;

  // opossum::GetTable table(table_name);
  // std::string t5 = table.table_name();
  // std::cout << " table_name= " << t5 << std::endl;

  // auto gt = std::make_shared<opossum::GetTable>(t5);
  // gt->execute();
  // std::cout << gt->get_output() << std::endl;

  return 0;
}
