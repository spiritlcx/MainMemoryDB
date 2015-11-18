#include "Semantic.hpp"

void Semantic::analysis() const {
	for(std::string relationname: query.relations){
		bool flag = false;
		for(Schema::Relation& relation : schema->relations){
			if(relationname == relation.name){
				flag = true;
			//	for(Schema::Relation::Attribute attribute : relation.attributes){
					
			//		for(std::pair<std::string, std::string> select : query.selectconditions){
			//			if(attribute.name == select.first){
			//			
			//			}	
			//		}
			//		for(std::pair<std::string, std::string> join : query.joinconditions){
			//			if(attribute.name )
			//		}
			//	}
			}
		}
	
		if(!flag){
			std::cout << relationname + " relation doesn't exist" << std::endl;
		}
	}
}

int main(){

	QueryParser queryParser;
	CreateParser createParser("schema.sql");	
	
	std::unique_ptr<Schema> schema; 
	
	try {
		schema = createParser.parse();
	}catch (ParserError& e) {
		std::cerr << e.what() << std::endl;
		return -1;
	}
	
	Semantic semantic(schema);
	
	while(true){
		try {
			Query query = queryParser.parse();
			semantic.setQuery(query);
			semantic.analysis();
			std::cout << "please do more queries if you want." << std::endl;
	
		} catch (ParserError& e) {
			std::cerr << e.what() << std::endl;
		}
	}

	return 0;
}
