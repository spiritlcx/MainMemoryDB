#include "project.hpp"
#include "Operation.hpp"
#include <ctime>
#include <chrono>
#include <ratio>


int main(int argc, char *argv[]){


	Customer *customer = new Customer();
	customer->init();
	relationTable.insert({"customer",customer});
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

	
	std::chrono::high_resolution_clock::time_point querystart = std::chrono::high_resolution_clock::now();


	std::vector<std::tuple<std::string,std::string,std::string>> expression;
	expression.push_back(std::make_tuple("=", "c_id", "5"));
	expression.push_back(std::make_tuple("=", "c_d_id", "5"));


	while(!scanner.end){
		Dataflow dataflow = scanner.scan(columns);
		SelectOperation::select(dataflow, expression);
	}

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
