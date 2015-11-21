#include "task5.hpp"
#include <cstdlib>
#include <dlfcn.h>

int main(int argc, char *argv[]){

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

			std::clock_t compilestart = std::clock();

			Query query = queryParser.parse();
			semantic.setQuery(query);
			semantic.analysis(relations);

			if(system("./compiletree.sh") != 0)
				continue;
			
			void* handle=dlopen("./tree_expression.so",RTLD_NOW);
			if (!handle) {
				std::cout << "bac" << std::endl;
				std::cerr << "error: " << dlerror() << std::endl;
				exit(1);
			}

			auto fn=reinterpret_cast<void* (*)(Customer*, Order*,Orderline*,Warehouse*,District*,Item*)>(dlsym(handle, "print"));
			if (!fn) {
				std::cerr << "error: " << dlerror() << std::endl;
				exit(1);
			}

			fn(customer, order,orderline,warehouse,district,item);


			if (dlclose(handle)) {
				std::cerr << "error: " << dlerror() << std::endl;
				exit(1);
			}

			if(system("./compilequery.sh") != 0)
				continue;

		
			void* handlequery = dlopen("./query_expression.so",RTLD_NOW);
			if (!handlequery) {
				std::cerr << "error: " << dlerror() << std::endl;
				exit(1);
			}
			auto fnn=reinterpret_cast<void (*)(Customer*, Order*, Orderline*, Warehouse*, District*,Item*)>(dlsym(handlequery, "query"));
			if (!fnn) {
				std::cerr << "error: " << dlerror() << std::endl;
				exit(1);
			}
		
			std::clock_t compilefinish = std::clock();

			std::clock_t querystart = std::clock();

			fnn(customer, order, orderline, warehouse, district,item);

			std::clock_t queryfinish = std::clock();

			std::cout << "Compilation time is ";
			std::cout << (compilefinish - compilestart) / (double) CLOCKS_PER_SEC<<std::endl;	


			std::cout << "Query time is ";
			std::cout << (queryfinish - querystart) / (double) CLOCKS_PER_SEC<<std::endl;;

			if (dlclose(handlequery)) {
				std::cerr << "error: " << dlerror() << std::endl;
				exit(1);
			}	
		} catch (ParserError& e) {
			std::cerr << e.what() << std::endl;
		} catch (SemanticError& e){
			std::cerr << e.what() << std::endl;
		}
	}


	return 0;
}
