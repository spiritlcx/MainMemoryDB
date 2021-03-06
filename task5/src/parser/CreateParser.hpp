#ifndef H_Parser_hpp
#define H_Parser_hpp

#include <exception>
#include <string>
#include <vector>
#include <memory>
#include <fstream>
#include <sstream>
#include "Schema.hpp"

class CreateParserError : std::exception {
   std::string msg;
   unsigned line;
   public:
   CreateParserError(unsigned line, const std::string& m) : msg(m), line(line) {}
   ~CreateParserError() throw() {}
   const char* what() const throw() {
      return msg.c_str();
   }
};

struct CreateParser {
   std::string fileName;
   std::ifstream in;
   enum class State : unsigned { Init, Create, Table, CreateTableBegin,
	   CreateTableEnd, TableName, Primary, Key, KeyListBegin, KeyName,
	   KeyListEnd, AttributeName, AttributeTypeInt, AttributeTypeChar,
	   CharBegin, CharValue, CharEnd, AttributeTypeVarchar, VarcharBegin,
	   VarcharValue, VarcharEnd, AttributeTypeNumeric, NumericBegin,
	   NumericValue1, NumericSeparator, NumericValue2, NumericEnd, AttributeTypeTimestamp,
	   Not, Null, Separator, Semicolon, Index, IndexName, IndexOn,
	   IndexTable, IndexBegin, IndexValue, IndexEnd};
   State state;
   CreateParser(const std::string& fileName) : fileName(fileName), state(State::Init) {}
   CreateParser(){}
   ~CreateParser() {};
   std::unique_ptr<Schema> parse();
   std::vector<std::string> split(std::string s, char delimit);

   private:
   void nextToken(unsigned line, const std::string& token, Schema& s);
};

#endif
