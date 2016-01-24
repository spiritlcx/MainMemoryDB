#include "project.hpp"
#include "Operation.hpp"
#include <ctime>
#include <chrono>
#include <ratio>


int main(int argc, char *argv[]){


	Customer *customer = new Customer();
	customer->init();
	
	Order *order = new Order();
	order->init();

	relationTable.insert({"customer",customer});
	relationTable.insert({"order",order});
/*
	Orderline *orderline = new Orderline();
	Warehouse *warehouse = new Warehouse();
	District *district = new District();
	Item *item = new Item();

	orderline->init();
	warehouse->init();
	district->init();
	item->init();
	relationTable.insert({"orderline",orderline});
	relationTable.insert({"warehouse",warehouse});
	relationTable.insert({"district",district});
	relationTable.insert({"item",item});
*/

	QueryParser queryParser;
	CreateParser createParser("schema.sql");	
	
	std::unique_ptr<Schema> schema; 
	
	try {
		schema = createParser.parse();
		for(Schema::Relation &relation : schema->relations){
			relations.insert({relation.name, relation});
			for(Schema::Relation::Attribute &attribute : relation.attributes){	
				attributes.insert({attribute.name, attribute});
				columnTable.insert({attribute.name, relationTable[relation.name]});
			}
		}
	}catch (CreateParserError& e) {
		std::cerr << e.what() << std::endl;
		return -1;
	}
	
	Semantic semantic(schema);

	ScanOperation scanner;

	std::vector<std::string> columns;
	columns.push_back("c_id");
	columns.push_back("c_d_id");
	columns.push_back("c_w_id");
	columns.push_back("c_balance");
	
	std::chrono::high_resolution_clock::time_point querystart = std::chrono::high_resolution_clock::now();


	std::vector<std::tuple<std::string,std::string,std::string>> selectexpression;
	selectexpression.push_back(std::make_tuple("=", "c_id", "5"));
	selectexpression.push_back(std::make_tuple("=", "c_d_id", "5"));

	std::vector<std::tuple<std::string,std::string,std::string>> selectexpression1;
	selectexpression1.push_back(std::make_tuple("=", "o_c_id", "5"));
	selectexpression1.push_back(std::make_tuple("=", "o_d_id", "5"));

	std::vector<std::tuple<std::string, std::string>> joinexpression;
	joinexpression.push_back(std::make_tuple("c_id","o_c_id"));
	joinexpression.push_back(std::make_tuple("c_d_id","o_d_id"));

	std::vector<std::string> probe;
	probe.push_back("o_id");
	probe.push_back("o_d_id");
	probe.push_back("o_w_id");
	probe.push_back("o_c_id");

	std::vector<std::string> printexpression;
	printexpression.push_back("c_id");
	printexpression.push_back("c_d_id");
	printexpression.push_back("c_w_id");
	printexpression.push_back("c_balance");
	printexpression.push_back("o_id");
	printexpression.push_back("o_c_id");
	printexpression.push_back("o_d_id");
		
	int sum = 0;

	while(!scanner.end){
	ScanOperation scanner1;
		Dataflow dataflow = scanner.scan(columns);
		dataflow = SelectOperation::select(dataflow, selectexpression);
		JoinOperation join;
		while(!scanner1.end){
	        Dataflow prob = scanner1.scan(probe);
			prob = SelectOperation::select(prob, selectexpression1);

			Dataflow result= join.hashjoin(dataflow, prob, joinexpression);
			sum+=result.size;
			PrintOperation::print(result, printexpression);
		}
	}

/*
typedef std::tuple<Integer,Integer> key_thashJoin0;
typedef std::tuple<Integer> key_tvaluehashJoin0;
struct key_hashhashJoin0 : public std::unary_function<key_thashJoin0, std::size_t>{
  std::size_t operator()(const key_thashJoin0& k) const{
    return std::get<0>(k).value+std::get<1>(k).value;
  }
};

struct key_equalhashJoin0 : public std::binary_function<key_thashJoin0, key_thashJoin0, bool>{
  bool operator()(const key_thashJoin0& v0, const key_thashJoin0& v1) const{
    return (
        std::get<0>(v0) == std::get<0>(v1)&&
          std::get<1>(v0) == std::get<1>(v1)
    );
  }
};

struct hashJoin0{
  Integer c_id;
};


std::unordered_multimap<key_thashJoin0, key_tvaluehashJoin0, key_hashhashJoin0,key_equalhashJoin0> hashmaphashJoin0;
for(unsigned i = 0; i <customer->size(); i++){
  hashmaphashJoin0.insert(std::make_pair(std::make_tuple(customer->get_c_id(i),customer->get_c_d_id(i)), std::make_tuple(customer->get_c_id(i))));
}
int sum = 0;
for(unsigned i = 0; i <order->size(); i++){
  auto range = hashmaphashJoin0.equal_range(std::make_tuple(order->get_o_c_id(i),order->get_o_d_id(i)));
  for(auto it = range.first; it != range.second; it++){
    hashJoin0 t;
    sum++;
    t.c_id = std::get<0>(it->second);
  }
}
std::cout << sum<< std::endl;

*/

		std::chrono::high_resolution_clock::time_point queryfinish = std::chrono::high_resolution_clock::now();
	
		std::chrono::duration<double> query_duration = std::chrono::duration_cast<std::chrono::duration<double>>(queryfinish-querystart);

		std::cout << "Query time is " << query_duration.count() << " seconds" << std::endl;

	

	/*	
	while(true){
		try {

			std::cout << "please write a query." << std::endl;

			Query query = queryParser.parse();
			semantic.setQuery(query);
			semantic.analysis(relations);


			std::chrono::high_resolution_clock::time_point querystart = std::chrono::high_resolution_clock::now();

	


			std::chrono::high_resolution_clock::time_point queryfinish = std::chrono::high_resolution_clock::now();
	
			std::chrono::duration<double> query_duration = std::chrono::duration_cast<std::chrono::duration<double>>(queryfinish-querystart);

			std::cout << "Query time is ";
			std::cout << query_duration.count() << " seconds" << std::endl;

		
		} catch (ParserError& e) {
			std::cerr << e.what() << std::endl;
		} catch (SemanticError& e){
			std::cerr << e.what() << std::endl;
		}
	}

*/
	return 0;
}
