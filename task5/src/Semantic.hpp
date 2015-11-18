#include "CreateParser.hpp"
#include "QueryParser.hpp"
#include <memory>

class Semantic{
public:
Semantic(std::unique_ptr<Schema> &schema) : schema(std::move(schema)){}
~Semantic(){}

void setQuery(const Query &query){
	this->query = query;
}

void analysis() const;
private:
std::unique_ptr<Schema> schema;
Query query;

};
