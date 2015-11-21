#include <iostream>
#include <vector>
#include <string>
#include "Query.hpp"

class ParserError : std::exception {
	std::string msg;
public:
	ParserError(const std::string& m) : msg(m){}
	~ParserError() throw() {}
	const char* what() const throw() {
		return msg.c_str();
	}
};

class Finish : std::exception {
public:
	Finish() {}
	~Finish() throw() {}
};

class QueryParser{

public:
QueryParser(){
	state = State::Init;
}
enum class State : unsigned {Init, Select, Selectname, From, Relationname, Where,  Whereleft, Whereequal, Wherequotation, Wherequotationvalue, Whereright};

const Query& parse();
void nextToken(std::string token);
private:
Query query;
State state;
std::string cname;
std::string rname;
};
