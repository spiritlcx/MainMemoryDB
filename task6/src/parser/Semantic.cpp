#include "Semantic.hpp"

void Semantic::analysis(std::unordered_map<std::string, Schema::Relation> &relations) const {

	for(const std::string &relationname : query.relations){
		auto it = relations.find(relationname);
		if(it == relations.end())
			throw SemanticError(relationname + " relation doesn't exist");
	}

	unsigned rnum = query.relations.size();
	std::unordered_map<std::string, Schema::Relation::Attribute> attributes;
	for(const std::string &relationname : query.relations){
		bool flag = false;
		auto itr = relations.find(relationname);
		for(Schema::Relation::Attribute attribute : itr->second.attributes){
			attributes.insert({attribute.name, attribute});
			for(const std::pair<std::string, std::string>& joincondition : query.joinconditions){
				if(flag)
					break;
				if(attribute.name == joincondition.first || attribute.name == joincondition.second){
					flag = true;
					break;
				}
			}
		}
		if(!flag && rnum != 1)
			throw SemanticError(relationname + " has no join condition");
	}
	

	for(const std::string& selectname : query.selectnames){
		auto itr = attributes.find(selectname);
		if(itr == attributes.end())
			throw SemanticError("Attribute " + selectname + " doesn't exist in any table");
	}

	for(const std::pair<std::string, std::string> &joincondition : query.joinconditions){
		auto itr = attributes.find(joincondition.first);
		if(itr == attributes.end())
			throw SemanticError("Attribute " + joincondition.first + " doesn't exist in any table");
		
		itr = attributes.find(joincondition.second);
		if(itr == attributes.end())
			throw SemanticError("Attribute " + joincondition.second + " doesn't exist in any table");
	
	
	}

	for(const std::pair<std::string, std::string> &selectcondition : query.selectconditions){
		auto itr = attributes.find(selectcondition.first);
		if(itr == attributes.end())
			throw SemanticError("Attribute " + selectcondition.first + " doesn't exist in any table");
		switch(itr->second.type	){
		case Types::Tag::Integer:
			try{
				std::stoi(selectcondition.second);
			}catch(std::exception const &e){
				std::string tem = selectcondition.second;
				tem[0] = ' ';
				tem[tem.size()-2]=' ';
				throw SemanticError(selectcondition.first +" is Integer type but "+tem + " is not a integer");
			}
			break;
		default:
			break;
		}
	}

	std::ofstream out;
	out.open("tree_expression.cpp");
	out << "#include \"task6.hpp\"" << std::endl;
	out << "extern \"C\" void print(Customer *customer, Order *order, Orderline *orderline, Warehouse *warehouse, District *district, Item *item){" << std::endl;
	out << "initialize();" << std::endl;
	int scanNumber = 0;
			
	std::unordered_map<std::string, std::string> operation;

	for(const std::string &relationname: query.relations){
		bool flag = false;

		auto it = relations.find(relationname);

		Schema::Relation& relation = it->second; 
		
		flag = true;
		out << "TableScan* tableScan" + std::to_string(scanNumber) + " = new TableScan(" + relationname + ");" << std::endl;
		operation[relationname] = "tableScan" + std::to_string(scanNumber);
		bool selectname = false;				
		for(const std::pair<std::string, std::string> &select : query.selectconditions){
			for(const Schema::Relation::Attribute &attribute : relation.attributes){	
				if(attribute.name == select.first){
					if(!selectname){
						out << "Selection* selection"+std::to_string(scanNumber) + " = new Selection(tableScan"+std::to_string(scanNumber) + ");" << std::endl;
						operation[relationname] = "selection" + std::to_string(scanNumber);
						selectname = true;
					}

					out << "selection" + std::to_string(scanNumber) + "->condition.push_back(std::make_pair(\""+select.first+"\", " + "\""+select.second + "\"));" << std::endl;							
				}
			}
		}
			
		if(!flag){
			throw SemanticError(relationname + " relation doesn't exist");
		}else{
			scanNumber++;
		}
	}

	int joinNumber = 0;
	if(query.relations.size() > 1){
		for(unsigned i = 0; i < query.relations.size() - 1; i++){
			for(const Schema::Relation& relation : schema->relations){
				if(query.relations[i] == relation.name){
					for(const Schema::Relation& relationA : schema->relations){
						if(query.relations[i+1] == relationA.name){
							bool joinname = false;

							for(const std::pair<std::string, std::string> &join : query.joinconditions){

								for(const Schema::Relation::Attribute &attribute : relation.attributes){	
									if(attribute.name == join.first){

										for(const Schema::Relation::Attribute &attributeA : relationA.attributes){	
											if(attributeA.name == join.second){

												if(!joinname){
													out << "HashJoin* hashJoin"+std::to_string(joinNumber) + " = new HashJoin("+ operation[relation.name] +", " + operation[relationA.name] + ");" << std::endl;
													joinname = true;
													out << "hashJoin" + std::to_string(joinNumber) + "->joinname = \"hashJoin" + std::to_string(joinNumber) + "\";" << std::endl;
												}
												out << "hashJoin" + std::to_string(joinNumber) + "->condition.push_back(std::make_pair(\""+join.first+"\", " + "\""+join.second + "\"));" << std::endl;							
												operation[relationA.name] = "hashJoin" + std::to_string(joinNumber);
											}
										}	
									}else if(attribute.name == join.second){
										for(Schema::Relation::Attribute attributeA : relationA.attributes){	
											if(attributeA.name == join.first){
												if(!joinname){
													out << "HashJoin* hashJoin"+std::to_string(joinNumber) + " = new HashJoin("+ operation[relation.name] +", " + operation[relationA.name] + ");" << std::endl;
													joinname = true;
													out << "hashJoin" + std::to_string(joinNumber) + "->joinname = \"hashJoin" + std::to_string(joinNumber)+"\";" << std::endl;
												}
												out << "hashJoin" + std::to_string(joinNumber) + "->condition.push_back(std::make_pair(\""+join.second+"\", " + "\""+join.first + "\"));" << std::endl;							
												operation[relationA.name] = "hashJoin" + std::to_string(joinNumber);
			
											}
										}	
									}
								}
							}
							joinNumber++;
						}
					}
				}
			}
		}
	}
	if(query.relations.size() == 0)
		return;

	out << "Print *print = new Print(" + operation[query.relations[query.relations.size()-1]]+");" <<std::endl;
	for(const std::string &printname : query.selectnames)
		out << "print->cnames.push_back(\"" + printname + "\");" << std::endl;
	out << "print->produce();" << std::endl;
	out << "finish();" << std::endl;

	out << "}" << std::endl;
	out.close();
}
