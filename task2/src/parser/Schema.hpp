#ifndef H_Schema_hpp
#define H_Schema_hpp

#include <vector>
#include <string>
#include "Types.hpp"

struct Schema {
   struct Relation {
      struct Attribute {
         std::string name;
         Types::Tag type;
         unsigned len;
         unsigned len1;
         unsigned len2;
         bool notNull;
         Attribute() : len(~0), notNull(true) {}
      };
      struct Index {
    	  std::string name;
    	  std::vector<unsigned> indexkey;
    	  Index(const std::string& name) : name(name) {}
      };
      std::string name;
      std::vector<Schema::Relation::Attribute> attributes;
      std::vector<unsigned> primaryKey;
      std::vector<Schema::Relation::Index> indices;
      Relation(const std::string& name) : name(name) {}
   };
   std::vector<Schema::Relation> relations;
   std::string toString() const;
   std::string toCpp() const;
};
#endif
