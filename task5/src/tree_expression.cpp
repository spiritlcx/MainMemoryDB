#include "task5.hpp"
extern "C" void print(Customer *customer, Order *order, Orderline *orderline, Warehouse *warehouse, District *district, Item *item){
initialize();
TableScan* tableScan0 = new TableScan(orderline);
Selection* selection0 = new Selection(tableScan0);
selection0->condition.push_back(std::make_pair("ol_number", "1"));
selection0->condition.push_back(std::make_pair("ol_o_id", "100"));
TableScan* tableScan1 = new TableScan(order);
HashJoin* hashJoin0 = new HashJoin(selection0, tableScan1);
hashJoin0->joinname = "hashJoin0";
hashJoin0->condition.push_back(std::make_pair("ol_o_id", "o_id"));
hashJoin0->condition.push_back(std::make_pair("ol_d_id", "o_d_id"));
hashJoin0->condition.push_back(std::make_pair("ol_w_id", "o_w_id"));
Print *print = new Print(hashJoin0);
print->cnames.push_back("o_id");
print->cnames.push_back("ol_dist_info");
print->produce();
finish();
}
