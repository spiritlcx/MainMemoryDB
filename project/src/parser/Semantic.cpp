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

}
