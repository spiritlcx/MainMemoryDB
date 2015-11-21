#include "task5.hpp"
std::ostream& operator<<(std::ostream& out,const Integer& value)
{
out << value.value;
return out;
}
extern "C" void query(Customer *customer, Order *order, Orderline *orderline, Warehouse *warehouse, District *district, Item *item){
typedef std::tuple<Integer,Integer,Integer> key_thashJoin0;
struct key_hashhashJoin0 : public std::unary_function<key_thashJoin0, std::size_t>{
	std::size_t operator()(const key_thashJoin0& k) const{
		return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value;
	}
};

struct key_equalhashJoin0 : public std::binary_function<key_thashJoin0, key_thashJoin0, bool>{
	bool operator()(const key_thashJoin0& v0, const key_thashJoin0& v1) const{
		return (
			std::get<0>(v0) == std::get<0>(v1)&&
			std::get<1>(v0) == std::get<1>(v1)&&
			std::get<2>(v0) == std::get<2>(v1)
		);
	}
};

struct hashJoin0{
	Integer o_id;
	Char<24> ol_dist_info;
};

std::vector<hashJoin0> hashJoin0s;
std::vector<hashJoin0> *hashJoin0ss;

std::unordered_multimap<key_thashJoin0, int, key_hashhashJoin0,key_equalhashJoin0> hashmaphashJoin0;
for(unsigned i = 0; i < orderline->size(); i++){
	if((*orderline)[i].ol_number==1 && (*orderline)[i].ol_o_id==100){
		hashmaphashJoin0.insert(std::make_pair(std::make_tuple((*orderline)[i].ol_o_id,(*orderline)[i].ol_d_id,(*orderline)[i].ol_w_id), i));
	}
}
int counthashJoin0 = 0;
for(unsigned i = 0; i < order->size(); i++){
	int temp = i;
	auto range = hashmaphashJoin0.equal_range(std::make_tuple((*order)[i].o_id,(*order)[i].o_d_id,(*order)[i].o_w_id));
	for(auto it = range.first; it != range.second; it++){
		hashJoin0 t;
		t.o_id = (*order)[i].o_id;
		t.ol_dist_info = (*orderline)[it->second].ol_dist_info;
		hashJoin0s.push_back(t);
		i = counthashJoin0;
		counthashJoin0++;
		hashJoin0ss=&hashJoin0s;
		std::cout << (*hashJoin0ss)[i].o_id <<", " <<(*hashJoin0ss)[i].ol_dist_info<< std::endl;
	}
	i = temp;
}
}
