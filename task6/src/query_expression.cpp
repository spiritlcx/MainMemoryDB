#include "task5.hpp"
#include "tbb/parallel_for.h"
#include "tbb/blocked_range.h"
#include "tbb/concurrent_unordered_map.h"
std::ostream& operator<<(std::ostream& out,const Integer& value)
{
out << value.value;
return out;
}
extern "C" void query(Customer *customer, Order *order, Orderline *orderline, Warehouse *warehouse, District *district, Item *item){
tbb::parallel_for(tbb::blocked_range<unsigned>(0,customer->size()),[&](const tbb::blocked_range<unsigned>& range){
for(unsigned i = range.begin(); i != range.end(); i++){
	if((*customer)[i].c_last=="BARBARBAR"){
		std::cout << (*customer)[i].c_id <<", " <<(*customer)[i].c_first <<", " <<(*customer)[i].c_middle <<", " <<(*customer)[i].c_last<< std::endl;
	}
}
});
}
