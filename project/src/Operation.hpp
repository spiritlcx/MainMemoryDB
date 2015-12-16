#include <tuple>
#include <unordered_map>
#include <iostream>
#include "Types.hpp"
#include <vector>
#include <string>

#define N 1024
//relation name and relation object
std::unordered_map<std::string, Schema::Relation> relations;
//attribute name and attribute object
std::unordered_map<std::string, Schema::Relation::Attribute> attributes;
//column(attribute) name and table pointer
std::unordered_map<std::string, Table*> columnTable;
//relation name and table pointer
std::unordered_map<std::string, Table*> relationTable;

struct Dataflow{

	Dataflow() : size(0), select(new int[N]){}
	Dataflow(const Dataflow &other): integerdata(other.integerdata), size(other.size), select(other.select){}
	Dataflow& operator=(Dataflow&& other){
		integerdata = other.integerdata;
		size = other.size;
		select = other.select;
		other.select = nullptr;
		other.size = 0;
		return *this;
	}

	Dataflow(Dataflow&& other){
		*this = std::move(other);
		other.select = nullptr;
		other.size = 0;
	}

	std::unordered_map<std::string, Integer*> integerdata;
	std::unordered_map<std::string, Timestamp*> timestampdata;
//	std::unordered_map<std::string, Numeric<,>*> integerdata;
//	std::unordered_map<std::string, Char<>*> integerdata;
//	std::unordered_map<std::string, VarChar<>*> integerdata;
	int size;
	int *select;
};

int scan_Integer(Table *table, Integer* &result, std::string column, int size, int offset){
	return table->getColumn(result, column, size, offset);
}

/*
int scan_char(){

}

int scan_varchar(){

}

int scan_timestamp(){

}

int scan_numeric(){

}
*/


class ScanOperation{
//scan the only needed columns
public:
	Dataflow scan(std::vector<std::string> columns){
		struct Dataflow dataflow;
		int size = 0;
		for(std::string column : columns){
			Schema::Relation::Attribute &attribute = attributes[column];
			switch(attribute.type){
			case Types::Tag::Integer:{
				Integer *result = nullptr;
				size = scan_Integer(columnTable[column], result, column, N, offset);
				for(int i = 0; i < size; i++){
					dataflow.select[i]=i;
				}
				dataflow.integerdata.insert({column,result});
				break;
			}
			default:
				break;
			}
		}
		offset += N;
		dataflow.size = size;
		if(size < N)
			end = true;
	//	return std::move(dataflow);
	return dataflow;
	}

	int offset = 0;
	bool end = false;
};

//size = 5
//output = {1,2,5,9,12}

int select_equal_Integer(int *output, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(input[output[i]] == value){
			output[count++] = output[i];
		}

//	output[count] = output[i];
//	count += (input[output[i]]==value);
	}
	return count;
}

int select_bigger_Integer(int *output, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(input[output[i]] > value){
			output[count] = output[i];
			count++;
		}
//		output[count] = i;
//		count += (input[i]==value);
	}
	return count;
}

int select_smaller_Integer(int *output, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(input[output[i]] < value){
			output[count] = output[i];
			count++;
		}
//		output[count] = i;
//		count += (input[i]==value);
	}
	return count;
}

//=(c_id, 12)
//=(c_first, 'chenxi')

class SelectOperation{
public:
static Dataflow select(Dataflow &dataflow, std::vector<std::tuple<std::string, std::string, std::string>> expression){
	int size = dataflow.size;
	for(std::tuple<std::string, std::string, std::string> exp : expression){
		if(std::get<0>(exp) == "="){
			switch(attributes[std::get<1>(exp)].type){
				case Types::Tag::Integer:
					size = select_equal_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
				break;

				case Types::Tag::Char:
					size = select_equal_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
				break;
				
				case Types::Tag::Varchar:
					size = select_equal_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
				break;
				
				case Types::Tag::Numeric:
					size = select_equal_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
				break;
				
				case Types::Tag::Timestamp:
					size = select_equal_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
				break;	
			}
		}else if(std::get<0>(exp) == "<"){

			switch(attributes[std::get<1>(exp)].type){
				case Types::Tag::Integer:
					size = select_smaller_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
					break;
				default:
					break;
			}
		}else if(std::get<0>(exp) == ">"){
			switch(attributes[std::get<1>(exp)].type){
				case Types::Tag::Integer:
					size = select_bigger_Integer(dataflow.select, dataflow.integerdata[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
					break;
				default:
					break;
			}
		}
 
	}

	for(int i = 0; i < size; i++){
		std::cout << dataflow.integerdata["c_id"][dataflow.select[i]]<< "  " << dataflow.integerdata["c_d_id"][dataflow.select[i]] <<std::endl;
	}
	return dataflow;
//	return std::move(dataflow);
}
};

/*
class ProjectOperation{
public:
	Dataflow project(Dataflow &dataflow, std::vector){

	}
};
*/
