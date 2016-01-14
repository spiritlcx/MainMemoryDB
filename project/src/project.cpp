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
//	std::cout << scan_Integer(customer, "c_id", 1024, 0) << std::endl;
/*
	Order *order = new Order();
	Orderline *orderline = new Orderline();
	Warehouse *warehouse = new Warehouse();
	District *district = new District();
	Item *item = new Item();

	customer->init();
	order->init();
	orderline->init();
	warehouse->init();
	district->init();
	item->init();
*/


//	tbb::task_scheduler_init init(4);

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
	
	std::chrono::high_resolution_clock::time_point querystart = std::chrono::high_resolution_clock::now();


	std::vector<std::tuple<std::string,std::string,std::string>> selectexpression;
	selectexpression.push_back(std::make_tuple("=", "c_id", "5"));
	selectexpression.push_back(std::make_tuple("=", "c_d_id", "5"));

	std::vector<std::tuple<std::string, std::string>> joinexpression;
	joinexpression.push_back(std::make_tuple("c_id","o_c_id"));
	joinexpression.push_back(std::make_tuple("c_d_id","o_d_id"));

	    std::vector<std::string> probe;
		probe.push_back("o_id");
		probe.push_back("o_d_id");
		probe.push_back("o_w_id");
		probe.push_back("o_c_id");
	int sum = 0;
	while(!scanner.end){

		Dataflow dataflow = scanner.scan(columns);
//		SelectOperation::select(dataflow, selectexpression);
		JoinOperation join;
		while(!join.end){
			Dataflow result= join.hashjoin(dataflow, probe, joinexpression);
			sum+=result.size;
		}
	}
	std::cout << sum<<std::endl;

			std::chrono::high_resolution_clock::time_point queryfinish = std::chrono::high_resolution_clock::now();
	
			std::chrono::duration<double> query_duration = std::chrono::duration_cast<std::chrono::duration<double>>(queryfinish-querystart);

			std::cout << "Query time is ";
			std::cout << query_duration.count() << " seconds" << std::endl;

	

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
