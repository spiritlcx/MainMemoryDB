#include "CreateParser.hpp"

#include <iostream>
#include <iterator>
#include <algorithm>
#include <cstdlib>

namespace keyword {
   const std::string Primary = "primary";
   const std::string Key = "key";
   const std::string Create = "create";
   const std::string Table = "table";
   const std::string Integer = "integer";
   const std::string Numeric = "numeric";
   const std::string Not = "not";
   const std::string Null = "null";
   const std::string Char = "char";
   const std::string Varchar = "varchar";
   const std::string Timestamp = "timestamp";
   const std::string Index = "index";
   const std::string On = "on";
}

namespace literal {
   const char ParenthesisLeft = '(';
   const char ParenthesisRight = ')';
   const char Comma = ',';
   const char Semicolon = ';';
}

Schema::Relation *currentrel = nullptr;
std::string indexName;

std::unique_ptr<Schema> CreateParser::parse() {
   std::string token;
   unsigned line=1;
   std::unique_ptr<Schema> s(new Schema());
   in.open(fileName.c_str());
   if (!in.is_open())
      throw CreateParserError(line, "cannot open file '"+fileName+"'");
   while (in >> token) {
//	  std::cout << token << std::endl;
      std::string::size_type pos;
      std::string::size_type prevPos = 0;

      while ((pos = token.find_first_of(",)(;", prevPos)) != std::string::npos) {
         nextToken(line, token.substr(prevPos, pos-prevPos), *s);
         nextToken(line, token.substr(pos,1), *s);
         prevPos=pos+1;
      }
      nextToken(line, token.substr(prevPos), *s);
      if (token.find("\n")!=std::string::npos)
         ++line;
   }
   in.close();
   return std::move(s);
}

static bool isIdentifier(const std::string& str) {
   if (
      str==keyword::Primary ||
      str==keyword::Key ||
      str==keyword::Table ||
      str==keyword::Create ||
      str==keyword::Integer ||
      str==keyword::Numeric ||
      str==keyword::Not ||
      str==keyword::Null ||
      str==keyword::Char
   )
      return false;
   return str.find_first_not_of("abcdefghijklmnopqrstuvwxyz_1234567890") == std::string::npos;
}

static bool isTableName(const std::string& str) {
	if(isIdentifier(str)){
		return true;
	} else {
		if(str.size() >= 3 && str[0] == '"' && str[str.size()-1]=='"' &&
				isIdentifier(str.substr(1, str.size()-2))){
			return true;
		}
	}
	return false;
}

static bool isInt(const std::string& str) {
   return str.find_first_not_of("0123456789") == std::string::npos;
}

void CreateParser::nextToken(unsigned line, const std::string& token, Schema& schema) {
	if (getenv("DEBUG"))
		std::cerr << line << ": " << token << std::endl;
   if (token.empty())
      return;
   std::string tok;
// to remember the current relation which index is creating on

   std::transform(token.begin(), token.end(), std::back_inserter(tok), tolower);
   switch(state) {
      case State::Semicolon: /* fallthrough */
      case State::Init:
         if (tok==keyword::Create)
            state=State::Create;
         else
            throw CreateParserError(line, "Expected 'CREATE', found '"+token+"'");
         break;
      case State::Create:
         if (tok==keyword::Table) {
        	 state=State::Table;
         } else if(tok==keyword::Index) {
        	 state=State::Index;
         } else {
            throw CreateParserError(line, "Expected 'TABLE', found '"+token+"'");
         }
         break;

      case State::Index:
    	  if(isIdentifier(tok)){
    		  indexName = token;
    		  state = State::IndexName;
    	  } else {
              throw CreateParserError(line, "Expected 'IndexName', found '"+token+"'");
    	  }
    	  break;
      case State::IndexName:
    	  if(token==keyword::On){
    		  state = State::IndexOn;
    	  }else {
              throw CreateParserError(line, "Expected 'On', found '"+token+"'");
    	  }
    	  break;
      case State::IndexOn:
    	  if (isIdentifier(tok) || isTableName(tok)) {
              std::vector<Schema::Relation> &relations = schema.relations;
              for(Schema::Relation &relation : relations){
            	  if(relation.name == token)
            		  currentrel = &relation;
              }
              if(currentrel == nullptr){
                  throw CreateParserError(line, "Table "+token+" doesn't exist");
              }
              currentrel->indices.push_back(Schema::Relation::Index(indexName));
			  state=State::IndexTable;
        	  //store the table name on which the index is created
    	  } else {
        	  //store the table name on which the index is created
              throw CreateParserError(line, "Expected TableName, found '"+token+"'");
    	  }
    	  break;
      case State::IndexTable:
    	  if(tok.size()==1 && tok[0]==literal::ParenthesisLeft) {
    		  state=State::IndexBegin;
    	  }else {
              throw CreateParserError(line, "Expected '(', found '"+token+"'");
    	  }
    	  break;
      case State::IndexBegin:
    	  if (isIdentifier(tok)) {
              struct AttributeNamePredicate {
                 const std::string& name;
                 AttributeNamePredicate(const std::string& name) : name(name) {}
                 bool operator()(const Schema::Relation::Attribute& attr) const {
                    return attr.name == name;
                 }
              };

              const auto& attributes = currentrel->attributes;
              AttributeNamePredicate p(token);

              auto it = std::find_if(attributes.begin(), attributes.end(), p);

              if (it == attributes.end())
                 throw CreateParserError(line, "'"+token+"' is not an attribute of '"+schema.relations.back().name+"'");
              (*currentrel).indices.back().indexkey.push_back(std::distance(attributes.begin(), it));

             state=State::IndexValue;
          } else {
             throw CreateParserError(line, "Expected key attribute, found '"+token+"'");
          }
    	  break;
      case State::IndexValue:
    	  if(tok.size()==1 && tok[0]==literal::Comma) {
    		  state=State::IndexBegin;
    	  } else if(tok.size()==1 && tok[0]==literal::ParenthesisRight){
    		  state=State::IndexEnd;
    	  }else {
    		  throw CreateParserError(line, "Expected ',' or ')', found" +token+"'");
    	  }
    	  break;
      case State::IndexEnd:
    	  if (tok.size()==1 && tok[0]==literal::Semicolon){
    		  state=State::Semicolon;
    		  currentrel = nullptr;
    	  } else {
    		  throw CreateParserError(line, "Expected ';' , found" +token+"'");
    	  }

    	  break;
      case State::Table:
         if (isIdentifier(tok) || isTableName(tok)) {
            state=State::TableName;
            schema.relations.push_back(Schema::Relation(token));
         } else {
            throw CreateParserError(line, "Expected TableName, found '"+token+"'");
         }
         break;
      case State::TableName:
         if (tok.size()==1 && tok[0]==literal::ParenthesisLeft)
            state=State::CreateTableBegin;
         else
            throw CreateParserError(line, "Expected '(', found '"+token+"'");
         break;
      case State::Separator: /* fallthrough */
      case State::CreateTableBegin:
         if (tok.size()==1 && tok[0]==literal::ParenthesisRight) {
            state=State::CreateTableEnd;
         } else if (tok==keyword::Primary) {
            state=State::Primary;
         } else if (isIdentifier(tok)) {
            schema.relations.back().attributes.push_back(Schema::Relation::Attribute());
            schema.relations.back().attributes.back().name = token;
            state=State::AttributeName;
         } else {
            throw CreateParserError(line, "Expected attribute definition, primary key definition or ')', found '"+token+"'");
         }
         break;
      case State::CreateTableEnd:
         if (tok.size()==1 && tok[0]==literal::Semicolon)
            state=State::Semicolon;
         else
            throw CreateParserError(line, "Expected ';', found '"+token+"'");
         break;
      case State::Primary:
         if (tok==keyword::Key)
            state=State::Key;
         else
            throw CreateParserError(line, "Expected 'KEY', found '"+token+"'");
         break;
      case State::Key:
         if (tok.size()==1 && tok[0]==literal::ParenthesisLeft)
            state=State::KeyListBegin;
         else
            throw CreateParserError(line, "Expected list of key attributes, found '"+token+"'");
         break;
      case State::KeyListBegin:
         if (isIdentifier(tok)) {
            struct AttributeNamePredicate {
               const std::string& name;
               AttributeNamePredicate(const std::string& name) : name(name) {}
               bool operator()(const Schema::Relation::Attribute& attr) const {
                  return attr.name == name;
               }
            };
            const auto& attributes = schema.relations.back().attributes;
            AttributeNamePredicate p(token);
            auto it = std::find_if(attributes.begin(), attributes.end(), p);
            if (it == attributes.end())
               throw CreateParserError(line, "'"+token+"' is not an attribute of '"+schema.relations.back().name+"'");
            schema.relations.back().primaryKey.push_back(std::distance(attributes.begin(), it));
            state=State::KeyName;
         } else {
            throw CreateParserError(line, "Expected key attribute, found '"+token+"'");
         }
         break;
      case State::KeyName:
         if (tok.size()==1 && tok[0] == literal::Comma)
            state=State::KeyListBegin;
         else if (tok.size()==1 && tok[0] == literal::ParenthesisRight)
            state=State::KeyListEnd;
         else
            throw CreateParserError(line, "Expected ',' or ')', found '"+token+"'");
         break;
      case State::KeyListEnd:
         if (tok.size()==1 && tok[0] == literal::Comma)
            state=State::Separator;
         else if (tok.size()==1 && tok[0] == literal::ParenthesisRight)
            state=State::CreateTableEnd;
         else
            throw CreateParserError(line, "Expected ',' or ')', found '"+token+"'");
         break;
      case State::AttributeName:
         if (tok==keyword::Integer) {
            schema.relations.back().attributes.back().type=Types::Tag::Integer;
            state=State::AttributeTypeInt;
         } else if (tok==keyword::Char) {
            schema.relations.back().attributes.back().type=Types::Tag::Char;
            state=State::AttributeTypeChar;
         } else if (tok==keyword::Varchar){
            schema.relations.back().attributes.back().type=Types::Tag::Varchar;
            state=State::AttributeTypeVarchar;
         } else if (tok==keyword::Numeric) {
            schema.relations.back().attributes.back().type=Types::Tag::Numeric;
            state=State::AttributeTypeNumeric;
         } else if (tok==keyword::Timestamp) {
            schema.relations.back().attributes.back().type=Types::Tag::Timestamp;
            state=State::AttributeTypeTimestamp;
         }
         else throw CreateParserError(line, "Expected type after attribute name, found '"+token+"'");
         break;
      case State::AttributeTypeChar:
         if (tok.size()==1 && tok[0]==literal::ParenthesisLeft)
            state=State::CharBegin;
         else
            throw CreateParserError(line, "Expected '(' after 'CHAR', found'"+token+"'");
         break;
      case State::CharBegin:
         if (isInt(tok)) {
            schema.relations.back().attributes.back().len=std::atoi(tok.c_str());
            state=State::CharValue;
         } else {
            throw CreateParserError(line, "Expected integer after 'CHAR(', found'"+token+"'");
         }
         break;
      case State::CharValue:
         if (tok.size()==1 && tok[0]==literal::ParenthesisRight)
            state=State::CharEnd;
         else
            throw CreateParserError(line, "Expected ')' after length of CHAR, found'"+token+"'");
         break;
      case State::AttributeTypeVarchar:
          if (tok.size()==1 && tok[0]==literal::ParenthesisLeft)
             state=State::VarcharBegin;
          else
             throw CreateParserError(line, "Expected '(' after 'VCHAR', found'"+token+"'");

    	  break;
      case State::VarcharBegin:
         if (isInt(tok)) {
            schema.relations.back().attributes.back().len=std::atoi(tok.c_str());
            state=State::VarcharValue;
         } else {
            throw CreateParserError(line, "Expected integer after 'VARCHAR(', found'"+token+"'");
         }
         break;

      case State::VarcharValue:
         if (tok.size()==1 && tok[0]==literal::ParenthesisRight)
            state=State::VarcharEnd;
         else
            throw CreateParserError(line, "Expected ')' after length of VCHAR, found'"+token+"'");
         break;

      case State::AttributeTypeNumeric:
         if (tok.size()==1 && tok[0]==literal::ParenthesisLeft)
            state=State::NumericBegin;
         else
            throw CreateParserError(line, "Expected '(' after 'NUMERIC', found'"+token+"'");
         break;
      case State::NumericBegin:
         if (isInt(tok)) {
            schema.relations.back().attributes.back().len1=std::atoi(tok.c_str());
            state=State::NumericValue1;
         } else {
            throw CreateParserError(line, "Expected integer after 'NUMERIC(', found'"+token+"'");
         }
         break;
      case State::NumericValue1:
         if (tok.size()==1 && tok[0]==literal::Comma)
            state=State::NumericSeparator;
         else
            throw CreateParserError(line, "Expected ',' after first length of NUMERIC, found'"+token+"'");
         break;
      case State::NumericValue2:
         if (tok.size()==1 && tok[0]==literal::ParenthesisRight)
            state=State::NumericEnd;
         else
            throw CreateParserError(line, "Expected ')' after second length of NUMERIC, found'"+token+"'");
         break;
      case State::NumericSeparator:
         if (isInt(tok)) {
            schema.relations.back().attributes.back().len2=std::atoi(tok.c_str());
            state=State::NumericValue2;
         } else {
            throw CreateParserError(line, "Expected second length for NUMERIC type, found'"+token+"'");
         }
         break;
      case State::CharEnd: /* fallthrough */
      case State::VarcharEnd:
      case State::NumericEnd: /* fallthrough */
      case State::AttributeTypeTimestamp:
      case State::AttributeTypeInt:
         if (tok.size()==1 && tok[0]==literal::Comma)
            state=State::Separator;
         else if (tok==keyword::Not)
            state=State::Not;
         else if (tok.size()==1 && tok[0]==literal::ParenthesisRight)
				state=State::CreateTableEnd;
         else throw CreateParserError(line, "Expected ',' or 'NOT NULL' after attribute type, found '"+token+"'");
         break;
      case State::Not:
         if (tok==keyword::Null) {
            schema.relations.back().attributes.back().notNull=true;
            state=State::Null;
         }
         else throw CreateParserError(line, "Expected 'NULL' after 'NOT' name, found '"+token+"'");
         break;
      case State::Null:
         if (tok.size()==1 && tok[0]==literal::Comma)
            state=State::Separator;
         else if (tok.size()==1 && tok[0]==literal::ParenthesisRight)
            state=State::CreateTableEnd;
         else throw CreateParserError(line, "Expected ',' or ')' after attribute definition, found '"+token+"'");
         break;
      default:
         throw CreateParserError(line, tok + "is not a valid symbol");
   }
}

std::vector<std::string> CreateParser::split(std::string s, char delimit){
	std::vector<std::string> names;
	std::size_t begin = 0;
	std::size_t end;
	while((end = s.find(delimit, begin)) != std::string::npos){
		names.push_back(s.substr(begin, end - begin));
		begin = end + 1;
	}
	names.push_back(s.substr(begin, s.size()-begin));
	return names;
}
