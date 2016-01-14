#include <tuple>
#include <unordered_map>
#include <iostream>
#include "Types.hpp"
#include <vector>
#include <string>

#define N 30000
//relation name and relation object
std::unordered_map<std::string, Schema::Relation> relations;
//attribute name and attribute object
std::unordered_map<std::string, Schema::Relation::Attribute> attributes;
//column(attribute) name and table pointer
std::unordered_map<std::string, Table*> columnTable;
//relation name and table pointer
std::unordered_map<std::string, Table*> relationTable;

struct Dataflow{

	Dataflow() : size(0){}
//	Dataflow(const Dataflow other) {std::cout << 22<<std::endl;}
/*	Dataflow& operator=(Dataflow&& other){

		integerdata = other.integerdata;
		size = other.size;
		select = other.select;
		other.select = nullptr;
		other.size = 0;
		return *this;
	}
*/

/*
	Dataflow(Dataflow&& other){
		*this = std::move(other);
		other.select = nullptr;
		other.size = 0;
	}
*/

//	~Dataflow(){
//		for(std::string col : dataorder){
//			delete [] (int*)integerdata[col];
//		}
//		delete [] select;
//	}

	std::unordered_map<std::string, void*> integerdata;
	std::vector<std::string> dataorder;
//	std::unordered_map<std::string, Timestamp*> timestampdata;
//	std::unordered_map<std::string, Numeric<,>*> integerdata;
//	std::unordered_map<std::string, Char<>*> integerdata;
//	std::unordered_map<std::string, VarChar<>*> integerdata;
	int size;
	int select[N];
};

int scan_Integer(Table *table, void** result, Schema::Relation::Attribute &attribute, int size, int offset){
	return table->getColumn(result, attribute, size, offset);
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

		for(int i = 0; i < N; i++){
			dataflow.select[i]=i;
		}

		for(std::string column : columns){
			Schema::Relation::Attribute &attribute = attributes[column];
			switch(attribute.type){
			case Types::Tag::Integer:{
				Integer *result = nullptr;
				size = scan_Integer(columnTable[column], (void **)&result, attribute, N, offset);

				dataflow.dataorder.push_back(column);
				dataflow.integerdata.insert({column,result});
				break;
			}
			case Types::Tag::Numeric:{

//				constexpr unsigned len1 = attribute.len1;
//				constexpr unsigned len2 = attribute.len2;
//				std::cout << len1 << " "<<len2<<std::endl;
				//Numeric<len1, len2> *result = nullptr;
				Numeric<45,19> *result;
				size = scan_Integer(columnTable[column],(void **)&result, attribute, N, offset);
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

int select_equal_Integer(int *output, void *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(*((Integer*)input+output[i]) == value){
			output[count++] = output[i];
		}

//	output[count] = output[i];
//	count += (input[output[i]]==value);
	}
	return count;
}

int select_bigger_Integer(int *output, void *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(*((Integer*)input+output[i]) > value){
			output[count] = output[i];
			count++;
		}
//		output[count] = i;
//		count += (input[i]==value);
	}
	return count;
}

int select_smaller_Integer(int *output, void *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(*((Integer*)input+output[i]) < value){
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
		for(unsigned j = 0; j < dataflow.dataorder.size(); j++)
			std::cout << *((Integer*)dataflow.integerdata[dataflow.dataorder[i]]+dataflow.select[i]) << " ";
		std::cout << std::endl;
//<< "  " << *((Integer*)dataflow.integerdata["c_d_id"]+dataflow.select[i])<< std::endl;//"  " << *((Numeric<45,19>*)dataflow.integerdata["c_balance"]+dataflow.select[i]) <<std::endl;
	}
	return dataflow;
//	return std::move(dataflow);
}
};



struct HashTable{
	int first[N];
	int next[N+1];
	int *values[10];
	std::vector<std::string> colorder;
/*
	HashTable(){
		for(int i = 0; i < 10; i++){
			values[i] = nullptr;
		}
	}

	~HashTable(){
		std::cout << colorder.size() << std::endl;
		for(int i = 0; i < 10; i++){

			if(values[i] != nullptr){
				delete [] values[i];
			}
		}
	}
*/
};

void hash(int *hashValue, void *key, int size){
	for(int i = 0; i < size; i++){
		hashValue[i] = (hashValue[i] + *((int*)key+i))%N;
	}
}

void hashTableInsert(int *groupId, HashTable &hashTable, int *hashValue, int size){
	
	for(int i = 0; i < size; i++){
		groupId[i] = i+1;
		hashTable.next[groupId[i]] = hashTable.first[hashValue[i]];
		hashTable.first[hashValue[i]] = groupId[i];
	}
}

void spread(void *value, int *groupId, void *inputvalue, int size){

	for(int i = 0; i < size; i++){
		*((int*)value+groupId[i]) = *((int*)inputvalue+i);
	}
}

void lookupInitial(int *pgroupId, int *first, int *hashValue, int size){
	for(int i = 0; i < size; i++){
		pgroupId[i] = first[hashValue[i]];
	}
}

void check(int *differ, int *toCheck, int *groupId, int *rgroupId, int *sgroupId, int *value, int *probekey, int m){
	for(int i = 0; i < m; i++){
		int index = groupId[toCheck[i]];
		if(probekey[toCheck[i]] != value[index]){
			differ[toCheck[i]] = 1;
		}
	}
}

void assign(int *differ, int *toCheck, int *groupId, int *rgroupId, int *sgroupId, int m){

	for(int i = 0; i < m ;i++){
		int index = groupId[toCheck[i]];
		if(differ[toCheck[i]] == 0){
			sgroupId[index] = rgroupId[toCheck[i]];
			rgroupId[toCheck[i]] = index;	
		}
	}
}

int slectMisses(int *toCheck, int *groupId, int *next, int m){
	int count = 0;
	for(int i = 0; i < m; i++){
		if(next[groupId[toCheck[i]]] != 0){
			toCheck[count++] = toCheck[i];
		}
	}
	return count;
}

void findNext(int *toCheck, int *next, int *groupId, int m){
	for(int i = 0; i < m; i++){
		groupId[toCheck[i]] = next[groupId[toCheck[i]]];
	}
}

void gather(int *result, int *groupId, int *probtable, int size){
	for(int i = 0; i < size; i++){
		result[groupId[i]] = probtable[i];
	}
}

class JoinOperation{
public:
	bool initial = true;
	bool built = false;
	HashTable hashTable;
	ScanOperation scanner;
	bool end = false;
	unsigned M = 0;
	Dataflow hashjoin(Dataflow &dataflow, std::vector<std::string> &probe, std::vector<std::tuple<std::string, std::string>> expression){
		if(!built){
			built = true;
			memset(hashTable.first, 0, N*sizeof(int));
			memset(hashTable.next, 0, (N+1)*sizeof(int));

			int hashValue[dataflow.size];
			memset(hashValue, 0, dataflow.size*sizeof(int));
		
			for(std::tuple<std::string, std::string> exp: expression){
				hash(hashValue, dataflow.integerdata[std::get<0>(exp)], dataflow.size);
			}

			int groupId[dataflow.size];
			hashTableInsert(groupId, hashTable, hashValue, dataflow.size);

			for(const std::tuple<std::string, std::string> &exp: expression){
				hashTable.values[M] = new int[dataflow.size+1];	
				hashTable.colorder.push_back(std::get<0>(exp));

				spread(hashTable.values[M], groupId, dataflow.integerdata[std::get<0>(exp)], dataflow.size);
				M++;
			}

			for(std::string column : dataflow.dataorder){
				hashTable.values[M] = new int[dataflow.size+1];
				if(std::find(hashTable.colorder.begin(), hashTable.colorder.end(),column) != hashTable.colorder.end())
					continue;
				hashTable.colorder.push_back(column);

				spread(hashTable.values[M], groupId, dataflow.integerdata[column], dataflow.size);
				M++;

			}
		}
		Dataflow prob = scanner.scan(probe);
		end = scanner.end;
	
		int probsize = prob.size;
		Dataflow result;

		if(prob.size == 0){
			return result;
		}

		int probehashValue[prob.size];
		memset(probehashValue, 0, prob.size*sizeof(int));
		for(std::tuple<std::string, std::string> exp : expression){
			hash(probehashValue, prob.integerdata[std::get<1>(exp)], prob.size);
		}
		int pgroupId[prob.size];
		int rgroupId[prob.size];
		int sgroupId[dataflow.size+1];
		memset(rgroupId, 0, prob.size*sizeof(int));
		memset(sgroupId, 0, (dataflow.size+1)*sizeof(int));

		lookupInitial(pgroupId, hashTable.first, probehashValue, prob.size);
		
		int toCheck[prob.size];
		for(int i = 0; i < probsize; i++)
			toCheck[i] = i;

		int m = probsize;
		while(m > 0){

			int differ[probsize];
			memset(differ, 0, probsize*sizeof(int));

			int i = 0;
			for(std::tuple<std::string, std::string> exp: expression){
				check(differ, toCheck, pgroupId, rgroupId, sgroupId, (int*)hashTable.values[i], (int*)prob.integerdata[std::get<1>(exp)], m);
				i++;
			}
			assign(differ, toCheck, pgroupId, rgroupId, sgroupId, m);
			m = slectMisses(toCheck, pgroupId, hashTable.next, m);
			findNext(toCheck, hashTable.next, pgroupId, m);
		}

		for(unsigned i = 0; i < prob.dataorder.size(); i++){
			std::string column = prob.dataorder[i];
			int *probtable = (int *)prob.integerdata[column];

			if(initial){
				hashTable.colorder.push_back(column);
				hashTable.values[M+i] = new int[dataflow.size+1];
			}

			gather((int*)hashTable.values[M+i], rgroupId, probtable, prob.size);
		}

		initial = false;

		int count = 0;

		for(int i = 0; i < probsize; i++){
			
			if(rgroupId[i] != 0){
//				result.select[count] = rgroupId[i];
				count++;
				int index = rgroupId[i];

				while(sgroupId[index] != 0){
					count++;
//					index = sgroupId[index];
					index = sgroupId[sgroupId[index]];
				}
			}
		}		

		result.size = count;

		if(count==0)
			return result;

		for(unsigned i = 0; i < hashTable.colorder.size(); i++){
			std::string column = hashTable.colorder[i];
			int *value = (int*)hashTable.values[i];
			result.dataorder.push_back(column);
			result.integerdata.insert({column, value});
		}
/*
		for(std::string col : result.dataorder){
			int *value = (int*)result.integerdata[col];
			for(int i = 0; i < result.size; i++){
				std::cout << value[result.select[i]] << " ";
			}
			std::cout << std::endl;
		}

		std::cout << "end" << std::endl;
*/
		return result;
	}
};


/*
class ProjectOperation{
public:
	Dataflow project(Dataflow &dataflow, std::vector){

	}
};
*/
