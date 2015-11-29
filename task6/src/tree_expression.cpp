#include "task5.hpp"
extern "C" void print(Customer *customer, Order *order, Orderline *orderline, Warehouse *warehouse, District *district, Item *item){
initialize();
TableScan* tableScan0 = new TableScan(customer);
Selection* selection0 = new Selection(tableScan0);
selection0->condition.push_back(std::make_pair("c_last", "\"BARBARBAR\""));
Print *print = new Print(selection0);
print->cnames.push_back("c_id");
print->cnames.push_back("c_first");
print->cnames.push_back("c_middle");
print->cnames.push_back("c_last");
print->produce();
finish();
}
