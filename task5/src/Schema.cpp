#include "Schema.hpp"

#include <sstream>

static std::string type(const Schema::Relation::Attribute& attr) {
   Types::Tag type = attr.type;
   switch(type) {
      case Types::Tag::Integer:
         return "Integer";
      case Types::Tag::Numeric: {
         std::stringstream ss;
         ss << "Numeric(" << attr.len1 << ", " << attr.len2 << ")";
         return ss.str();
    	  return "Numeric";
      }
      case Types::Tag::Char: {
         std::stringstream ss;
         ss << "Char(" << attr.len << ")";
         return ss.str();
      }
      case Types::Tag::Timestamp: {
    	 return "Timestamp";
      }
      case Types::Tag::Varchar: {
          std::stringstream ss;
          ss << "Varchar(" << attr.len << ")";
          return ss.str();
        }
   }
   throw;
}



std::string Schema::toString() const {
   std::stringstream out;
   for (const Schema::Relation& rel : relations) {
      out << rel.name << std::endl;
      out << "\tPrimary Key:";
      for (unsigned keyId : rel.primaryKey)
         out << ' ' << rel.attributes[keyId].name;
      out << std::endl;
      for(Schema::Relation::Index index : rel.indices){
    	  out << "\tindex " + index.name + ": ";
    	  for(unsigned indexId : index.indexkey){
    		  out << rel.attributes[indexId].name + " ";
    	  }
    	  out << std::endl;
      }
      for (const auto& attr : rel.attributes)
         out << '\t' << attr.name << '\t' << type(attr) << (attr.notNull ? " not null" : "") << std::endl;
   }
   return out.str();
}

std::string Schema::toCpp() const {
	std::stringstream out;

	out << "void sumchar(char *array){" << std::endl;
	out << "\tint total = 0;" << std::endl;
	out << "\tfor(char c : array){" << std::endl;
	out << "\t\ttotal+=c;" << std::endl;
	out << "\t}" << std::endl;
	out << "}" << std::endl;

	for (const Schema::Relation& rel : relations) {
		int keyNum = rel.primaryKey.size();
		int count = 1;

		out << "namespace N"+rel.name+"{" << std::endl;
		if(rel.primaryKey.size() != 0){
			out << "\ttypedef std::tuple<";
			for(unsigned key : rel.primaryKey){
				switch(rel.attributes[key].type){
				case Types::Tag::Integer:
					out << "Integer";
					break;
				case Types::Tag::Char:
					out << "Char<" + std::to_string(rel.attributes[key].len) + ">";
					break;
				case Types::Tag::Numeric:
					out << "Numeric<" + std::to_string(rel.attributes[key].len1) + "," + std::to_string(rel.attributes[key].len2) + ">";
					break;
				case Types::Tag::Timestamp:
					out << "Timestamp";
					break;
				case Types::Tag::Varchar:
					out << "Varchar<" + std::to_string(rel.attributes[key].len) + ">";
					break;
				}
				if(count < keyNum){
					out << ",";
					count++;
				}
			}
			out << "> key_t;" << std::endl;

			count = 0;
			out << "\tstruct key_hash : public std::unary_function<key_t, std::size_t>{" << std::endl;
			out << "\t\tstd::size_t operator()(const key_t& k) const{" << std::endl;
			out << "\t\t\treturn ";
			while(count < keyNum){
				switch(rel.attributes[rel.primaryKey[count]].type){
					case Types::Tag::Integer:
						out << "std::get<" + std::to_string(count) + ">(k).value";
						break;
					case Types::Tag::Char:
						out << "sumchar(std::get<" + std::to_string(count) + ">(k).value)";
						break;
					case Types::Tag::Varchar:
						out << "sumchar(std::get<" + std::to_string(count) + ">(k).value)";
						break;
					case Types::Tag::Numeric:
						out << "std::get<" + std::to_string(count) + ">(k).value";
						break;
					case Types::Tag::Timestamp:
						out << "std::get<" + std::to_string(count) + ">(k).value";
						break;
				}
				if(count < keyNum - 1){
					out << "+";
				}
				count++;
			}
			out << ";" << std::endl;
			out <<"\t\t}" << std::endl;
			out <<"\t};" << std::endl;

			out <<"\tstruct key_equal : public std::binary_function<key_t, key_t, bool>{" << std::endl;

			out << "\t\tbool operator()(const key_t& v0, const key_t& v1) const{" << std::endl;
			out << "\t\t\treturn (" << std::endl;
			count = 0;
			while(count < keyNum){
				out << "\t\t\t\tstd::get<" + std::to_string(count) + ">(v0) == std::get<" + std::to_string(count)+">(v1)";
				if(count < keyNum - 1){
					out << "&&" << std::endl;
				}else{
					out << std::endl;
				}
				count++;
			}
			out << "\t\t\t);" << std::endl;
			out <<"\t\t}" << std::endl;
			out <<"\t};" << std::endl;
		}
		out << "struct " + rel.name + '{' << std::endl;

		out << "\tstruct Attribute{" << std::endl;
	    out << "\t\tstd::string name;" << std::endl;
	    out << "\t\tTypes::Tag type;" << std::endl;
	    out << "\t\tunsigned len;" << std::endl;
	    out << "\t\tunsigned len1;" << std::endl;
	    out << "\t\tunsigned len2;" << std::endl;
	    out << "\t\tbool notNull;" << std::endl;
	    out << "\t\tAttribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2"
	    		") : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}" << std::endl;
	    out << "\t};" << std::endl;


		out << "\tstruct Row{" << std::endl;
		for(Schema::Relation::Attribute attribute : rel.attributes){
			switch(attribute.type){
			case Types::Tag::Integer:
				out << "\t\tInteger ";
				break;
			case Types::Tag::Char:
				out << "\t\tChar<" + std::to_string(attribute.len) + "> ";
				break;
			case Types::Tag::Varchar:
				out << "\t\tVarchar<" + std::to_string(attribute.len) + "> ";
				break;
			case Types::Tag::Numeric:
				out << "\t\tNumeric<" + std::to_string(attribute.len1) + "," + std::to_string(attribute.len2) + "> ";
				break;
			case Types::Tag::Timestamp:
				out << "\t\tTimestamp ";
				break;
			}
			out << attribute.name + ';' <<std::endl;
		}
		out << "\t};" << std::endl;
		if(rel.primaryKey.size() != 0){
			out << "\ttypedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;" << std::endl;
		}
		out <<"\tstd::list<Row> " + rel.name + "s;" << std::endl;
		out <<"\tstd::vector<Attribute> attributes;" << std::endl;
		out <<"\tstd::vector<unsigned> primaryKey;" << std::endl;
		if(rel.primaryKey.size() != 0){
			out <<"\tmap_t m;"<< std::endl;
		}
		out << "\tvoid init(){" << std::endl;
		for(unsigned key : rel.primaryKey){
			out << "\t\tprimaryKey.push_back(" + std::to_string(key) + ");" << std::endl;
		}
		out << std::endl;
		for(Schema::Relation::Attribute attribute : rel.attributes){
			if(attribute.type == Types::Tag::Integer){
				out << "\t\tattributes.push_back(Attribute(\"" + attribute.name + "\",Types::Tag::Integer, 0, 0, 0));"  << std::endl;
			}else if(attribute.type == Types::Tag::Char){
				out << "\t\tattributes.push_back(Attribute(\"" + attribute.name + "\",Types::Tag::Char, " + std::to_string(attribute.len)+", 0, 0));"  << std::endl;
			}else if(attribute.type == Types::Tag::Varchar){
				out << "\t\tattributes.push_back(Attribute(\"" + attribute.name + "\",Types::Tag::Varchar, "+ std::to_string(attribute.len) + ", 0, 0));"  << std::endl;
			}else if(attribute.type == Types::Tag::Numeric){
				out << "\t\tattributes.push_back(Attribute(\"" + attribute.name + "\",Types::Tag::Numeric, "+std::to_string(attribute.len1)+", "+std::to_string(attribute.len2)+", 0));"  << std::endl;
			}else if(attribute.type == Types::Tag::Timestamp){
				out << "\t\tattributes.push_back(Attribute(\"" + attribute.name + "\",Types::Tag::Timestamp, 0, 0, 0));" << std::endl;
			}
		}

		out << std::endl;
		out << "\t\tstd::string path = \"data/tpcc_" + rel.name + ".tbl\";" << std::endl;

		out << "\t\tstd::ifstream f(path);" << std::endl;;
		out << "\t\tstd::string line;" << std::endl;
		out << "\t\tParser parser;" << std::endl;
		out << "\t\twhile(!f.eof() && f >> line){" << std::endl;
		out << "\t\t\tstd::vector<std::string> data = parser.split(line, '|');" << std::endl;
		out << "\t\t\tRow row;" << std::endl;

		count = 0;
		for(Schema::Relation::Attribute attribute : rel.attributes){
			out << "\t\t\trow." +attribute.name+ " =";
			switch(attribute.type){
			case Types::Tag::Char:
				out << "Char<"+std::to_string(attribute.len)+">::castString(data["+std::to_string(count)+"].c_str(), data["+std::to_string(count)+"].length());" << std::endl;
				count++;
				break;
			case Types::Tag::Varchar:
				out << "Varchar<"+std::to_string(attribute.len)+">::castString(data["+std::to_string(count)+"].c_str(), data["+std::to_string(count)+"].length());" << std::endl;
				count++;
				break;
			case Types::Tag::Integer:
				out << "Integer::castString(data["+std::to_string(count)+"].c_str(), data["+std::to_string(count)+"].length());" << std::endl;
				count++;
				break;
			case Types::Tag::Numeric:
				out << "Numeric<"+std::to_string(attribute.len1) +',' + std::to_string(attribute.len2)+">::castString(data["+std::to_string(count)+"].c_str(), data["+std::to_string(count)+"].length());" << std::endl;
				count++;
				break;
			case Types::Tag::Timestamp:
				out << "Timestamp::castString(data["+std::to_string(count)+"].c_str(), data["+std::to_string(count)+"].length());" << std::endl;
				count++;
				break;
			}
		}

		out << "\t\t\t" + rel.name +'s' + ".push_back(row);" << std::endl;
		out << std::endl;
		if(rel.primaryKey.size() != 0){
			out << "\t\t\tm[std::make_tuple(";
			count = 0;
			for(unsigned key : rel.primaryKey){
				out << "row." + rel.attributes[key].name;
				if(count < keyNum - 1){
					out << ",";
				}
				count++;
			}

			out << ")] = row;" << std::endl;
		}
		out << "\t\t}" <<std::endl;


		out << "\t}" << std::endl;
		if(rel.primaryKey.size() != 0){
			out << "\tRow *find(std::tuple<";
			count = 1;
			for(unsigned key : rel.primaryKey){
				switch(rel.attributes[key].type){
				case Types::Tag::Integer:
					out << "Integer";
					break;
				case Types::Tag::Char:
					out << "Char<" + std::to_string(rel.attributes[key].len) + ">";
					break;
				case Types::Tag::Numeric:
					out << "Numeric<" + std::to_string(rel.attributes[key].len1) + "," + std::to_string(rel.attributes[key].len2) + ">";
					break;
				case Types::Tag::Timestamp:
					out << "Timestamp";
					break;
				case Types::Tag::Varchar:
					out << "Varchar<" + std::to_string(rel.attributes[key].len) + ">";
					break;
				}
				if(count < keyNum){
					out << ",";
					count++;
				}
			}


			out << "> tuple){" << std::endl;
			out << "\t\tauto itr = m.find(tuple);" << std::endl;
			out << "\t\tif(itr!=m.end()){" << std::endl;
			out << "\t\t\treturn &itr->second;" << std::endl;
			out << "\t\t}" << std::endl;
			out << "\t\telse{" << std::endl;
			out << "\t\t\treturn nullptr;" << std::endl;
			out << "\t\t}" << std::endl;
			out << "\t}" << std::endl;
		}

		out << "\tvoid insert(";
		count = 0;
		for(Schema::Relation::Attribute attribute : rel.attributes){
			switch(attribute.type){
			case Types::Tag::Integer:
				out << "Integer ";
				break;
			case Types::Tag::Char:
				out << "Char<" + std::to_string(attribute.len) + "> ";
				break;
			case Types::Tag::Numeric:
				out << "Numeric<" + std::to_string(attribute.len1) + "," + std::to_string(attribute.len2) + "> ";
				break;
			case Types::Tag::Timestamp:
				out << "Timestamp ";
				break;
			case Types::Tag::Varchar:
				out << "Varchar<" + std::to_string(attribute.len) + "> ";
				break;
			}
			out << attribute.name;
			if(count < rel.attributes.size() - 1){
				out << ",";
			}
			count++;
		}

		out << "){" << std::endl;
		out << "\t\t\tRow row;" << std::endl;

		for(Schema::Relation::Attribute attribute : rel.attributes){
			out << "\t\t\trow." +attribute.name+ " = " + attribute.name + ";" << std::endl;
			out << "\t\t\t" + rel.name +'s' + ".push_back(row);" << std::endl;
		}
		out <<"\t}"<<std::endl;

		out << "\tvoid insertAttribute(Attribute attribute){" << std::endl;
		out << "\t\tattributes.push_back(attribute);" << std::endl;

		out << "\t}" << std::endl;


		out << "\tvoid deleteAttribute(){" << std::endl;
//		out << "\t\tattributes.push_back(attribute);" << std::endl;
		out << "\t}" << std::endl;


		out << "\tstd::vector<Attribute> getAttributes(){" << std::endl;
		out << "\t\treturn attributes;" << std::endl;
		out << "\t}" << std::endl;
		out <<"};" << std::endl;
		out <<"}" << std::endl;

	}
	return out.str();
}
