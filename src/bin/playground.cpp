#include <iostream>

#include "../lib/operators/get_table.hpp"
#include "../lib/storage/storage_manager.hpp"
#include "../lib/storage/table.hpp"
#include "../lib/utils/assert.hpp"

int main() {
  opossum::Assert(true, "We can use opossum files here :)");
  std::cout << "+++++++++++++ Starting playground: +++++++++++++" << std::endl;

  // Create table with one entry
  auto& sm = opossum::StorageManager::get();
  auto t1 = std::make_shared<opossum::Table>();
  auto t2 = std::make_shared<opossum::Table>(4);

  sm.add_table("first_table", t1);
  sm.add_table("second_table", t2);

  auto t3 = sm.get_table("first_table");
  auto t4 = sm.get_table("second_table");

  std::cout << "Table One = " << &t3;
  std::cout << " && Table Two = " << &t4 << std::endl;

  opossum::GetTable table("first_table");
  std::string t5 = table.table_name();

  std::cout << " table_name " << t5 << std::endl;
  ;

  // Get table from StorageManager by tablename

  return 0;
}
