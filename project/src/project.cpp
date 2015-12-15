#include "project.hpp"
#include "tbb/tbb.h"
#include <ctime>
#include <chrono>
#include <ratio>

#define N 1024

std::unordered_map<std::string, Schema::Relation> relations;
std::unordered_map<std::string, Schema::Relation::Attribute> attributes;
std::unordered_map<std::string, Table*> columnTable;
std::unordered_map<std::string, Table*> relationTable;

struct integer_vector{
	std::string name;
	Integer * data;
};

struct Dataflow{
	std::vector<integer_vector> integerdata;
//	std::vector<Integer *> integerdata;
//	std::vector<Integer *> integerdata;
//	std::vector<Integer *> integerdata;
//	std::vector<Integer *> integerdata;
};

int scan_Integer(Table *table, Integer *result, std::string column, int size, int offset){
//	std::cout << table <<std::endl;
	return table->getColumn(result, column, size, offset);
}

Dataflow scan(std::vector<std::string> columns){
	struct Dataflow dataflow;
	for(std::string column : columns){
		Schema::Relation::Attribute &attribute = attributes[column];
		switch(attribute.type){
		case Types::Tag::Integer:{
			Integer *result = new Integer[N];
			scan_Integer(columnTable[column], result, column, N, 0);
			struct integer_vector vector;
			vector.name = column;
			vector.data = result;
			dataflow.integerdata.push_back(vector);
			break;
		}
		default:
			break;
		}
	}
	std::cout << dataflow.integerdata.size() << std::endl;
	return dataflow;
}

/*
int scan_char(){

}

int scan_varchar(){

}

int scan_timestamp(){

}

int scan_numeric(){

}
*/

int select_equal_Integer(int *output, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
//		if(input[i] == value){
//			output[count] = i;
//			count++;
//		}
		output[count] = i;
		count += (input[i]==value);
	}
	return count;
}

Dataflow select(Dataflow dataflow, std::vector<string> expression){
	
}

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
	int size = 1024;
	int *output = new int[size];
	Integer *input = new Integer[size];
//	std::cout<<select_equal_Integer(output, input, 0, size)<<std::endl;

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
	
	std::vector<std::string> columns;
	columns.push_back("c_id");
	columns.push_back("c_d_id");
	scan(columns);

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
