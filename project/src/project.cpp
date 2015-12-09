#include "project.hpp"
#include "tbb/tbb.h"
#include <ctime>
#include <chrono>
#include <ratio>

#define N 1024

struct student{
	int a[10] = {1,2,3,4,5,6,7,8,9,10};
	int b[10] = {1,2,3,4,5,6,7,8,9,10};
	int* operator[](int index){
		if(index == 0)
		return a;
		else
		return b;
	}
};

int main(int argc, char *argv[]){

	student c;
	//c.a = new int[10];
	//c.b=new int[10];
	std::cout << c[0][5] <<std::endl;

	Customer *customer = new Customer();
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

	for(int i = 0; i < customer->size(); i+=N){
	
	}


//	tbb::task_scheduler_init init(4);

	QueryParser queryParser;
	CreateParser createParser("schema.sql");	
	
	std::unique_ptr<Schema> schema; 
	
	std::unordered_map<std::string, Schema::Relation> relations;

	try {
		schema = createParser.parse();
		for(Schema::Relation &relation : schema->relations){
			relations.insert({relation.name, relation});
		}
	}catch (CreateParserError& e) {
		std::cerr << e.what() << std::endl;
		return -1;
	}
	
	Semantic semantic(schema);
	
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


	return 0;
}
