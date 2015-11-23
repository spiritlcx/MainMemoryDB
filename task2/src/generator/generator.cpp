/*
 * generator.cpp
 *
 *  Created on: Oct 23, 2015
 *      Author: spiritlcx
 */
#include <iostream>
#include <memory>
#include "../parser/Parser.hpp"
#include "../parser/Schema.hpp"
#include "Generator.hpp"

//int main(int argc, char* argv[]){
//   if (argc != 2) {
//      std::cerr << "usage: " << argv[0] << " <schema file>" << std::endl;
//      return -1;
//   }
//
//   Parser p(argv[1]);
//   try {
//	   std::unique_ptr<Schema> schema = p.parse();
////	   std::vector<Schema::Relation> relations = schema->relations;
//	   std::cout << schema->toCpp() << std::endl;
//   } catch (ParserError& e) {
//       std::cerr << e.what() << std::endl;
//   }
//   return 0;
//}
