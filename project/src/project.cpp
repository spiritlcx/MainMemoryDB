#include "project.hpp"
#include "Operation.hpp"
#include <ctime>
#include <chrono>
#include <ratio>

//relation name and relation object
std::unordered_map<std::string, Schema::Relation> relations;
//attribute name and attribute object
std::unordered_map<std::string, Schema::Relation::Attribute> attributes;
//column(attribute) name and table pointer
std::unordered_map<std::string, Table*> columnTable;
//relation name and table pointer
std::unordered_map<std::string, Table*> relationTable;

Operation *buildTree(Query &query, std::unique_ptr<Schema> &schema){
	PrintOperation *printOperation = new PrintOperation();
	if(query.relations.size() > 1){
		bool firstjoin = true;
		JoinOperation *formerJoin = nullptr;
		for(unsigned i = 0; i < query.relations.size() - 1; i++){
			JoinOperation *joinOperation = new JoinOperation();
			std::string leftTable = query.relations[i];
			std::string rightTable = query.relations[i+1];
			
			for(const std::pair<std::string, std::string> &join : query.joinconditions){
				if(columnTable[std::get<0>(join)]->name == leftTable && columnTable[std::get<1>(join)]->name == rightTable){
					joinOperation->expression.push_back(std::make_tuple(std::get<0>(join), std::get<1>(join)));
				}else if(columnTable[std::get<0>(join)]->name == rightTable && columnTable[std::get<1>(join)]->name == leftTable){
					joinOperation->expression.push_back(std::make_tuple(std::get<1>(join), std::get<0>(join)));
				}
			}

			if(firstjoin){
				bool leftFlag = false;
				bool rightFlag = false;
				SelectOperation *selectOperationLeft = nullptr;
				SelectOperation *selectOperationRight = nullptr;
				for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
					if(columnTable[std::get<1>(select)]->name == leftTable){
						if(!leftFlag){
							selectOperationLeft = new SelectOperation();
							joinOperation->left = selectOperationLeft;
							leftFlag = true;
							selectOperationLeft->attributes = attributes;

						}
						if(leftFlag){
							selectOperationLeft->expression.push_back(std::make_tuple(std::get<0>(select), std::get<1>(select), std::get<2>(select)));
							selectOperationLeft->attributes = attributes;
						}
					}

					if(columnTable[std::get<1>(select)]->name == rightTable){
						if(!rightFlag){
							selectOperationRight = new SelectOperation();
							joinOperation->right = selectOperationRight;
							selectOperationRight->attributes = attributes;

							rightFlag = true;
						}
						if(rightFlag){
							selectOperationRight->expression.push_back(std::make_tuple(std::get<0>(select), std::get<1>(select), std::get<2>(select)));
							selectOperationRight->attributes = attributes;
						}
					}
				}

				ScanOperation *scanOperationLeft = new ScanOperation();
				scanOperationLeft->columnTable = columnTable; 
				scanOperationLeft->attributes = attributes;

				for(const std::string &selectname : query.selectnames){
					if(columnTable[selectname]->name != leftTable)
						continue;
					if(std::find(scanOperationLeft->columns.begin(), scanOperationLeft->columns.end(), selectname) != scanOperationLeft->columns.end())
						continue;
					scanOperationLeft->columns.push_back(selectname);
				}
				for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
					if(columnTable[std::get<1>(select)]->name != leftTable)
						continue;
					if(std::find(scanOperationLeft->columns.begin(), scanOperationLeft->columns.end(), std::get<1>(select)) != scanOperationLeft->columns.end())
						continue;
					scanOperationLeft->columns.push_back(std::get<1>(select));
				}
				for(const std::pair<std::string, std::string> &join : query.joinconditions){
					if(columnTable[std::get<0>(join)]->name == leftTable){
						if(std::find(scanOperationLeft->columns.begin(), scanOperationLeft->columns.end(), std::get<0>(join)) != scanOperationLeft->columns.end())
							continue;
						scanOperationLeft->columns.push_back(std::get<0>(join));
					}
					if(columnTable[std::get<1>(join)]->name == leftTable){
						if(std::find(scanOperationLeft->columns.begin(), scanOperationLeft->columns.end(), std::get<1>(join)) != scanOperationLeft->columns.end())
							continue;
						scanOperationLeft->columns.push_back(std::get<1>(join));
					}
				}

				if(!leftFlag){
					joinOperation->left = scanOperationLeft;
				}else{
					selectOperationLeft->left = scanOperationLeft;
				}


				ScanOperation *scanOperationRight = new ScanOperation();
				scanOperationRight->attributes = attributes;
				scanOperationRight->columnTable = columnTable;


				for(const std::string &selectname : query.selectnames){
					if(columnTable[selectname]->name != rightTable)
						continue;
					if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), selectname) != scanOperationRight->columns.end())
						continue;
					scanOperationRight->columns.push_back(selectname);
				}
				for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
					if(columnTable[std::get<1>(select)]->name != rightTable)
						continue;
					if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), std::get<1>(select)) != scanOperationRight->columns.end())
						continue;
					scanOperationRight->columns.push_back(std::get<1>(select));
				}

				for(const std::pair<std::string, std::string> &join : query.joinconditions){

					if(columnTable[std::get<0>(join)]->name == rightTable){
						if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), std::get<0>(join)) != scanOperationRight->columns.end())
							continue;
						scanOperationRight->columns.push_back(std::get<0>(join));
					}

					if(columnTable[std::get<1>(join)]->name == rightTable){
						if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), std::get<1>(join)) != scanOperationRight->columns.end())
							continue;
						scanOperationRight->columns.push_back(std::get<1>(join));
					}
				}

				if(!rightFlag){
					joinOperation->right = scanOperationRight;
				}else{
					selectOperationRight->left = scanOperationRight;
				}

				firstjoin = false;
			}else{
				bool rightFlag = false;
				SelectOperation *selectOperation = nullptr;
				joinOperation->left = formerJoin;
				for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
					if(columnTable[std::get<1>(select)]->name ==  rightTable){
						if(!rightFlag){
							selectOperation = new SelectOperation();
							selectOperation->attributes = attributes;

							joinOperation->right = selectOperation;
							rightFlag = true;
						}
						if(rightFlag){
							selectOperation->expression.push_back(std::make_tuple(std::get<0>(select), std::get<1>(select), std::get<2>(select)));
							selectOperation->attributes = attributes;
						}
					}
				}

				ScanOperation *scanOperationRight = new ScanOperation();
				scanOperationRight->attributes = attributes;
				scanOperationRight->columnTable = columnTable;


				for(const std::string &selectname : query.selectnames){
					if(columnTable[selectname]->name != rightTable)
						continue;
					if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), selectname) != scanOperationRight->columns.end())
						continue;

					scanOperationRight->columns.push_back(selectname);
				}
				for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
					if(columnTable[std::get<1>(select)]->name != rightTable)
						continue;
					if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), std::get<1>(select)) != scanOperationRight->columns.end())
						continue;
					scanOperationRight->columns.push_back(std::get<1>(select));
				}

				for(const std::pair<std::string, std::string> &join : query.joinconditions){

					if(columnTable[std::get<0>(join)]->name == rightTable){
						joinOperation->left->push_back(std::get<1>(join));
						if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), std::get<0>(join)) != scanOperationRight->columns.end())
							continue;
						scanOperationRight->columns.push_back(std::get<0>(join));
					}

					if(columnTable[std::get<1>(join)]->name == rightTable){
						joinOperation->left->push_back(std::get<0>(join));
						if(std::find(scanOperationRight->columns.begin(), scanOperationRight->columns.end(), std::get<1>(join)) != scanOperationRight->columns.end())
							continue;
						scanOperationRight->columns.push_back(std::get<1>(join));
					}
				}

				if(!rightFlag){
					joinOperation->right = scanOperationRight;						
				}else{
					selectOperation->left = scanOperationRight;
				}
			}
			formerJoin = joinOperation;
			printOperation->left = formerJoin;
		}

		for(const std::string &selectname : query.selectnames){
			printOperation->columns.push_back(selectname);	
			printOperation->left->push_back(selectname);
		}

		return printOperation;
	}else{
		std::string leftTable = query.relations[0];
		SelectOperation *selectOperation = nullptr;
		bool selectionFlag = false;
		for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
			if(columnTable[std::get<1>(select)]->name ==  leftTable){
				if(!selectionFlag){
					selectOperation = new SelectOperation();
					selectOperation->attributes = attributes;
					printOperation->left = selectOperation;
					selectionFlag = true;
				}
				if(selectionFlag){
					selectOperation->expression.push_back(std::make_tuple(std::get<0>(select), std::get<1>(select), std::get<2>(select)));
					selectOperation->attributes = attributes;

				}
			}
		}
		ScanOperation *scanOperation = new ScanOperation();
		scanOperation->attributes = attributes;
		scanOperation->columnTable = columnTable;


		for(const std::string &selectname : query.selectnames){
			scanOperation->columns.push_back(selectname);
			printOperation->columns.push_back(selectname);
		}
		for(const std::tuple<std::string, std::string, std::string> &select : query.selectconditions){
			scanOperation->columns.push_back(std::get<1>(select));
		}
		if(!selectionFlag){
			printOperation->left = scanOperation;						
		}else{
			selectOperation->left = scanOperation;	
		}

		return printOperation;
	}
	return printOperation;
}

typedef std::tuple<Integer,Integer,Integer> key_thashJoin1;
typedef std::tuple<Integer> key_tvaluehashJoin1;
struct key_hashhashJoin1 : public std::unary_function<key_thashJoin1, std::size_t>{
  std::size_t operator()(const key_thashJoin1& k) const{
    return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value;
  }
};

struct key_equalhashJoin1 : public std::binary_function<key_thashJoin1, key_thashJoin1, bool>{
  bool operator()(const key_thashJoin1& v0, const key_thashJoin1& v1) const{
    return (
        std::get<0>(v0) == std::get<0>(v1)&&
          std::get<1>(v0) == std::get<1>(v1)&&
          std::get<2>(v0) == std::get<2>(v1)
    );
  }
};

struct hashJoin1{
  Integer o_id;
};


std::unordered_multimap<key_thashJoin1, key_tvaluehashJoin1, key_hashhashJoin1,key_equalhashJoin1> hashmaphashJoin1;
typedef std::tuple<Integer> key_thashJoin0;
typedef std::tuple<> key_tvaluehashJoin0;
struct key_hashhashJoin0 : public std::unary_function<key_thashJoin0, std::size_t>{
  std::size_t operator()(const key_thashJoin0& k) const{
    return std::get<0>(k).value;
  }
};

struct key_equalhashJoin0 : public std::binary_function<key_thashJoin0, key_thashJoin0, bool>{
  bool operator()(const key_thashJoin0& v0, const key_thashJoin0& v1) const{
    return (
        std::get<0>(v0) == std::get<0>(v1)
    );
  }
};

struct hashJoin0{
  Integer o_id;
  Integer o_d_id;
  Integer o_w_id;
};


std::unordered_multimap<key_thashJoin0, int, key_hashhashJoin0,key_equalhashJoin0> hashmaphashJoin0;


int main(int argc, char *argv[]){
	Customer *customer = new Customer();
	customer->init();
	
	Order *order = new Order();
	order->init();

	relationTable.insert({"customer",customer});
	relationTable.insert({"order",order});

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

	
	while(true){
		try {

			std::cout << "please write a query." << std::endl;

			Query query = queryParser.parse();
			semantic.setQuery(query);
			semantic.analysis(relations);
			
			
			Operation *operation = buildTree(query, semantic.schema);

			std::chrono::high_resolution_clock::time_point querystart = std::chrono::high_resolution_clock::now();

			operation->execute();

/*

        for(unsigned i = 0; i <customer->size(); i++){
        	hashmaphashJoin0.insert(std::make_pair(std::make_tuple(customer->c_id[i]), 1));
        }
        
		for(unsigned i = 0; i <order->size(); i++){
        	auto range = hashmaphashJoin0.equal_range(std::make_tuple(order->o_c_id[i]));
        	for(auto it = range.first; it != range.second; it++){
        		hashmaphashJoin1.insert(std::make_pair(std::make_tuple(order->o_id[i],order->o_d_id[i],order->o_w_id[i]), std::make_tuple(order->o_id[i])));
        	} 
        }
        int sum = 0;
        for(unsigned i = 0; i <orderline->size(); i++){
			if(orderline->ol_number[i]==1 && orderline->ol_o_id[i]==100){
            	auto range = hashmaphashJoin1.equal_range(std::make_tuple(orderline->ol_o_id[i],orderline->ol_d_id[i],orderline->ol_w_id[i]));
            	for(auto it = range.first; it != range.second; it++){
					std::cout << std::get<0>(it->second)<< std::endl;
            	}
			}
        }



			for(unsigned i = 0; i < customer->size(); i++){
				if(customer->c_id[i] ==5 && customer->c_d_id[i]==5 && customer->c_w_id[i]==5){
					std::cout << customer->c_id[i]<<std::endl;
				}
			}
*/
			std::chrono::high_resolution_clock::time_point queryfinish = std::chrono::high_resolution_clock::now();
	
		std::chrono::duration<double> query_duration = std::chrono::duration_cast<std::chrono::duration<double>>(queryfinish-querystart);

		std::cout << "Query time is " << query_duration.count() << " seconds" << std::endl;

	
		} catch (ParserError& e) {
			std::cerr << e.what() << std::endl;
		} catch (SemanticError& e){
			std::cerr << e.what() << std::endl;
		}
	}


	return 0;
}
