#include "QueryParser.hpp"
#include <iostream>

namespace keyword{
	const std::string Select = "select";
	const std::string From = "from";
	const std::string Where = "where";
	const std::string And = "and";

}

namespace literal {
	const std::string Comma = ",";
	const std::string Semicolon = ";";
	const std::string Equal = "=";
	const std::string Quotation = "'";
}

static bool isIdentifier(const std::string& str) {
	if (
		str==keyword::Select ||
		str==keyword::From ||
		str==keyword::Where ||
		str==keyword::And
	)
	return false;
	return str.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_1234567890") == std::string::npos;
}

static bool isColumnName(const std::string& str){
	return str.find_first_not_of("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_") == std::string::npos;

}

const Query& QueryParser::parse(){
	query.relations.clear();
	query.selectnames.clear();
	query.joinconditions.clear();
	query.selectconditions.clear();

	std::string token;
	while(std::cin >> token){
		std::string::size_type pos;
		std::string::size_type prevPos = 0;

		 while ((pos = token.find_first_of(",;='", prevPos)) != std::string::npos) {
		 	nextToken(token.substr(prevPos, pos-prevPos));
			if(token.substr(prevPos, pos-prevPos) == ";")
				return query;

			nextToken(token.substr(pos,1));
			if(token.substr(pos, 1) == ";")
				return query;
			prevPos=pos+1;
		}

		nextToken(token.substr(prevPos));	
	}
	return query;
}

void QueryParser::nextToken(std::string token){
	if(token.empty())
		return;

	switch(state){
	case State::Init:
		if(token == keyword::Select){
			state = State::Select;
		}else {
			state = State::Init;
			throw ParserError("select is expected, found " + token );
		}
		break;
	case State::Select:
		if(isIdentifier(token)){
			query.selectnames.push_back(token);
			state= State::Selectname;
		}else {
			state = State::Init;
			throw ParserError("column name is expected, found " + token );
		}
	
		break;
	case State::Selectname:
		if(token == keyword::From){
			state = State::From;
		} else if(token == literal::Comma){
			state = State::Select;
		}else {
			state = State::Init;
			throw ParserError(", or from is expected, found "+ token );
		}

		break;
	case State::From:
		if(isIdentifier(token)){
			query.relations.push_back(token);
			state = State::Relationname;
		}else {
			state = State::Init;
			throw ParserError("relation name is expected, found " + token );
		}
		break;
 	case State::Relationname:
		if(token == keyword::Where){
			state = State::Where;
		}else if(token == literal::Comma){
			state = State::From;
		}else if(token == literal::Semicolon){
			state = State::Init;
		}else {
			state = State::Init;
			throw ParserError(", or where or ; is expected, found " + token);
		}
		break;
	case State::Where:
		if(isIdentifier(token)){
			state = State::Whereleft;
			cname = token;
		}else {
			state = State::Init;
			throw ParserError("column name is expected, found " + token);
		}
		break;
	case State::Whereleft:
		if(token == literal::Equal){
			state = State::Whereequal;	
		}else {
			state = State::Init;
			throw ParserError("'=' is expected, found " + token);
		}
		break;
 	case State::Whereequal:
		if(token == literal::Quotation){
			state = State::Wherequotation;
		}else if(isIdentifier(token)){
			if(isColumnName(token)){
				if(query.relations.size() == 1){
					state = State::Init;
					throw ParserError("Only one table, join is not available");
				}

				query.joinconditions.push_back(std::make_pair(cname, token));
			}else{
				query.selectconditions.push_back(std::make_pair(cname, token));	
			}
		state = State::Whereright;
		}else {
			state = State::Init;
			throw ParserError("column name or value is expected, found " + token);
		}
 		break;
	case State::Wherequotation:
		if(isIdentifier(token)){
			rname = token;
			state = State::Wherequotationvalue;
		}else {
			state = State::Init;
			throw ParserError("value is expected, found " + token);
		}
		break;
	case State::Wherequotationvalue:
		if(token == literal::Quotation){
			query.selectconditions.push_back(std::make_pair(cname, "\\\"" + rname + "\\\""));
			state = State::Whereright;
		}else{
			state = State::Init;
			throw ParserError("' is expected, found " + token);	
		}
		break;
	case State::Whereright:
		if(token == literal::Semicolon){	
			state = State::Init;
		}else if(token == keyword::And){
			state = State::Where;
		}else{
			state = State::Init;
			throw ParserError("; or and is expected, found " + token);
		}
		break;
	default:
		break;
	}
}

