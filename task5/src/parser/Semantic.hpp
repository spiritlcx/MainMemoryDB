#include "CreateParser.hpp"
#include "QueryParser.hpp"
#include <unordered_map>
#include <memory>
#include <fstream>


class SemanticError : std::exception{
	std::string msg;
public:
	SemanticError(const std::string& m) : msg(m){}
	~SemanticError() throw() {}
	const char* what() const throw() {
		return msg.c_str();
	}

};

class Semantic{
public:
Semantic(std::unique_ptr<Schema> &schema) : schema(std::move(schema)){}
~Semantic(){}

void setQuery(const Query &query){
	this->query = query;
}

void analysis(std::unordered_map<std::string, Schema::Relation> &relations) const;
private:
std::unique_ptr<Schema> schema;
Query query;

};
