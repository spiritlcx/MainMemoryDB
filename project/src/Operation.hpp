#include <tuple>
#include <unordered_map>
#include <iostream>
#include "Types.hpp"
#include <vector>
#include <string>

#define LEN 32
#define N 8000
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
	std::unordered_map<std::string, void*> data;
	std::vector<std::string> colorder;
	std::unordered_map<std::string, Schema::Relation::Attribute> colattribute;
	~Dataflow(){
		if(select != nullptr){
			delete [] select;
		}

		if(name != "join")
			return;
		for(std::string col : colorder){
			delete [] (Integer*)data[col];
		}
	}	
	
	int size;
	int *select = nullptr;
	bool flag = false;
	std::string name = "";
};

int scan_column(Table *table, void** result, std::string column, int size, int offset){
	return table->getColumn(result, column, size, offset);
}

class Operation{
public:
	std::string name;
	Operation *left = nullptr;
	Operation *right = nullptr;
	virtual Dataflow *execute() = 0;
	virtual void refresh(){
	
	}
	bool end = false;
	std::vector<std::string> usefulcolumn;
	void push_back(std::string column){
		usefulcolumn.push_back(column);
		Operation *t = left;	
		while(t != nullptr){
			t->usefulcolumn.push_back(column);
			t = t->left;
		}
	}
};

class ScanOperation : public Operation{
//scan the only needed columns
public:
	int offset = 0;
	std::vector<std::string> columns;
	virtual void refresh(){
		offset = 0;
		end = false;
	}
	virtual Dataflow *execute(){
		return scan(columns);
	}
	
	Dataflow *scan(std::vector<std::string> columns){
		struct Dataflow *dataflow = new Dataflow();
		int remain = 0;
		dataflow->select = new int[N];
		for(int i = 0; i < N; i++){
			dataflow->select[i]=i;
		}
	
		for(std::string column : columns){	
			Schema::Relation::Attribute &attribute = attributes[column];
			dataflow->colorder.push_back(column);
			dataflow->colattribute.insert({column, attribute});
	
			void *result = nullptr;
			remain = scan_column(columnTable[column], (void **)&result, column, N, offset);
	
			dataflow->data.insert({column,result});
		}
	
		offset += N;
		if(remain > 0){
			dataflow->size = N;
		}else{
			dataflow->size = N + remain;
			end = true;
		}

	return dataflow;
	}
};

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

class SelectOperation : public Operation{
public:
	std::vector<std::tuple<std::string, std::string, std::string>> expression;
	virtual void refresh(){
		left->refresh();
	}
	virtual Dataflow *execute(){
		Dataflow *dataflow = left->execute();
		end = left->end;
		return select(dataflow, expression);
	}
	Dataflow *select(Dataflow *dataflow, std::vector<std::tuple<std::string, std::string, std::string>> expression){
		int size = dataflow->size;
		for(std::tuple<std::string, std::string, std::string> exp : expression){
			if(std::get<0>(exp) == "="){
				switch(attributes[std::get<1>(exp)].type){
					case Types::Tag::Integer:
						size = select_equal_Integer(dataflow->select, (Integer*)dataflow->data[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
						break;

					case Types::Tag::Numeric:
						size = select_equal_Integer(dataflow->select, (Integer*)dataflow->data[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
						break;
					default:
						break;
				}
			}else if(std::get<0>(exp) == "<"){

				switch(attributes[std::get<1>(exp)].type){
					case Types::Tag::Integer:
						size = select_smaller_Integer(dataflow->select, (Integer*)dataflow->data[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
						break;
					default:
						break;
				}
			}else if(std::get<0>(exp) == ">"){
				switch(attributes[std::get<1>(exp)].type){
					case Types::Tag::Integer:
						size = select_bigger_Integer(dataflow->select, (Integer*)dataflow->data[std::get<1>(exp)],std::stoi(std::get<2>(exp)),size);
						break;
					default:
						break;
				}
			}
		}

		dataflow->size = size;
		return dataflow;
	}
};

struct HashTable{
	int *first = nullptr;
	int *next = nullptr;
	void *values[20];
	std::vector<std::string> colorder;
	HashTable(){
		for(int i = 0;i < 20; i++){
			values[i] = nullptr;
		}
	}
/*
	~HashTable(){
		for(unsigned i = 0; i < colorder.size(); i++){
			if(values[i] != nullptr)
				delete [] (Integer*)values[i];
		}
		delete [] first;
		delete [] next;
	}
*/	
};

void hash(int *select, int *hashValue, Integer *key, int size){
	for(int i = 0; i < size; i++){
		hashValue[i] += key[select[i]].value;
	}
}

void bucket(int *hashValue, int size, int n){
	for(int i = 0; i < size; i++){
		hashValue[i] = hashValue[i] % n;
	}
}

void hashTableInsert(int *select, int *groupId, HashTable &hashTable, int *hashValue, int size){
	
	for(int i = 0; i < size; i++){
		groupId[i] = i+1;
		hashTable.next[groupId[i]] = hashTable.first[hashValue[i]];
		hashTable.first[hashValue[i]] = groupId[i];
	}
}

void spread_Integer(int *select, Integer *value, int *groupId, Integer *inputvalue, int size){

	for(int i = 0; i < size; i++){
		value[groupId[i]] = inputvalue[select[i]];
	}
}

void spread_Numeric(int *select, Integer *value, int *groupId, Integer *inputvalue, int size){

	for(int i = 0; i < size; i++){
		value[groupId[i]] = inputvalue[select[i]];
	}
}

int lookupInitial(int *toCheck, int *pgroupId, int *first, int *hashValue, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(first[hashValue[i]] != 0){
			toCheck[count++] = i;
			pgroupId[i] = first[hashValue[i]];
		}
	}
	return count;
}

void check(int *select, int *differ, int *toCheck, int *groupId, int *rgroupId, int *sgroupId, Integer *value, Integer *probekey, int m){
	for(int i = 0; i < m; i++){
		int index = groupId[toCheck[i]];
		if(probekey[select[toCheck[i]]] != value[index]){
			differ[toCheck[i]] = 1;
		}
	}
}

void build(int *differ, int *toCheck, int *groupId, int *rgroupId, int *sgroupId, int m){

	for(int i = 0; i < m ;i++){
		int index = groupId[toCheck[i]];
		if(differ[toCheck[i]] == 0){
			sgroupId[index] = rgroupId[toCheck[i]];
			rgroupId[toCheck[i]] = index;
		}
	}
}

int selectMisses(int *toCheck, int *groupId, int *next, int m){
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

int gather(int *select, int *toAssign, Integer *newvalue, Integer *value, int index, int size){
	for(int i = 0; i < size; i++){
		newvalue[index++] = value[select[toAssign[i]]];
	}
	return index;
}

int gather(int *select, int *toAssign, Integer *newvalue, Integer *value, int *groupId, int index, int size){
	for(int i = 0; i < size; i++){
		newvalue[index++] = value[groupId[toAssign[i]]];
	}
	return index;
}

class JoinOperation : public Operation{
public:
	bool built = false;
	bool newPhase = true;
	HashTable hashTable;
	Dataflow *leftData = nullptr;
	Dataflow *rightData = nullptr;
	unsigned M = 0;

	int sum = 0;
	int rightsum = 0;

	std::vector<std::tuple<std::string, std::string>> expression;
	virtual Dataflow *execute(){

		if(right->end){
			right->refresh();
			newPhase = true;
			built = false;
			M = 0;

//			for(unsigned i = 0; i < hashTable.colorder.size(); i++){
//				if(hashTable.values[i] != nullptr)
//					delete [] (Integer*)hashTable.values[i];
//			}
//			if(hashTable.first != nullptr)
//				delete [] hashTable.first;
//			if(hashTable.next != nullptr)
//				delete [] hashTable.next;
		}

		if(newPhase){
			newPhase = false;
			if(leftData != nullptr)
				delete leftData;
			leftData = left->execute();
			sum += leftData->size;
			if(leftData->size == 0){
				newPhase = true;
				if(left->end)
					end = true;
				return new Dataflow();
			}
		}
		if(rightData != nullptr){
			delete rightData;
		}
		rightData = right->execute();

		rightsum += rightData->size;

		if(left->end && right->end){
			end = true;	
		}

		if(rightData->size == 0)
			return new Dataflow();
		return hashjoin(leftData, rightData, expression);
	}
	Dataflow *hashjoin(Dataflow *dataflow, Dataflow *prob, std::vector<std::tuple<std::string, std::string>> expression){

		if(!built){
			built = true;
			hashTable.first = new int[dataflow->size];
			hashTable.next = new int[dataflow->size+1];
			memset(hashTable.first, 0, dataflow->size*sizeof(int));
			memset(hashTable.next, 0, (dataflow->size+1)*sizeof(int));
			int *hashValue = new int[dataflow->size];

			memset(hashValue, 0, dataflow->size*sizeof(int));

			for(const std::tuple<std::string, std::string> &exp: expression){
				hash(dataflow->select, hashValue, (Integer*)dataflow->data[std::get<0>(exp)], dataflow->size);
			}

			bucket(hashValue, dataflow->size, dataflow->size);
			int *groupId = new int[dataflow->size];
			hashTableInsert(dataflow->select, groupId, hashTable, hashValue, dataflow->size);
			delete [] hashValue;
			for(const std::tuple<std::string, std::string> &exp: expression){
				hashTable.values[M] = new Integer[dataflow->size+1];	
				hashTable.colorder.push_back(std::get<0>(exp));

				spread_Integer(dataflow->select, (Integer*)hashTable.values[M], groupId, (Integer*)dataflow->data[std::get<0>(exp)], dataflow->size);
				M++;
			}

			for(const std::string &column : dataflow->colorder){
				if(std::find(hashTable.colorder.begin(), hashTable.colorder.end(),column) != hashTable.colorder.end())
					continue;
				hashTable.values[M] = new Integer[dataflow->size+1];
				hashTable.colorder.push_back(column);

				spread_Integer(dataflow->select, (Integer*)hashTable.values[M], groupId, (Integer*)dataflow->data[column], dataflow->size);
				M++;

			}
			delete [] groupId;
		}

		int probsize = prob->size;
		Dataflow *result = new Dataflow();
		result->name = "join";
		result->flag = true;
		if(probsize == 0){
			return result;
		}
		int probehashValue[probsize];
		memset(probehashValue, 0, probsize*sizeof(int));
		
		for(const std::tuple<std::string, std::string> &exp : expression){
	//std::cout << std::get<1>(exp)<<std::endl;

			hash(prob->select, probehashValue, (Integer*)prob->data[std::get<1>(exp)], probsize);
		}

		bucket(probehashValue, probsize, dataflow->size);
		int pgroupId[probsize];
		int rgroupId[probsize];
		int *sgroupId = new int[dataflow->size+1];
		memset(pgroupId, 0, probsize*sizeof(int));
		memset(rgroupId, 0, probsize*sizeof(int));
		memset(sgroupId, 0, (dataflow->size+1)*sizeof(int));
		int toCheck[probsize];
		for(int i = 0; i < probsize; i++)
			toCheck[i] = i;
		int m =	lookupInitial(toCheck, pgroupId, hashTable.first, probehashValue, probsize);
		while(m > 0){
			int differ[probsize];
			memset(differ, 0, probsize*sizeof(int));
			int i = 0;
			for(const std::tuple<std::string, std::string> &exp: expression){
				check(prob->select, differ, toCheck, pgroupId, rgroupId, sgroupId, (Integer*)hashTable.values[i], (Integer*)prob->data[std::get<1>(exp)], m);
				i++;
			}
			build(differ, toCheck, pgroupId, rgroupId, sgroupId, m);
			m = selectMisses(toCheck, pgroupId, hashTable.next, m);
			findNext(toCheck, hashTable.next, pgroupId, m);
		}

		int count = 0;

//count the number of match
		for(int i = 0; i < probsize; i++){			
			if(rgroupId[i] != 0){
				count++;
				int index = rgroupId[i];
				while(sgroupId[index] != 0){
					count++;
					index = sgroupId[index];
				}
			}
		}
		result->size = count;
		if(count==0)
			return result;

		result->select = new int[count];
		for(int i = 0; i < count; i++){
			result->select[i] = i;
		}
		for(unsigned j = 0; j < prob->colorder.size(); j++){
			std::string column = prob->colorder[j];	
			if(std::find(usefulcolumn.begin(), usefulcolumn.end(), column) == usefulcolumn.end())
				continue;

			int toAssign[probsize];
			memset(toAssign, 0, probsize*sizeof(int));
			int matchnum = 0;

			for(int i = 0; i < probsize; i++){
				if(rgroupId[i] != 0)
					toAssign[matchnum++] = i;
			}

			int tgroupId[probsize];
			for(int i = 0; i < probsize; i++)
				tgroupId[i] = rgroupId[i];
			Integer* value = (Integer*)prob->data[column];
			Integer* newvalue = new Integer[count];
			int index = 0;
			gather(prob->select, toAssign, newvalue, value, index, matchnum);
			index += matchnum;
			while(matchnum > 0){
				matchnum = selectMisses(toAssign, tgroupId, sgroupId, matchnum);
				findNext(toAssign, sgroupId, tgroupId, matchnum);
				gather(prob->select, toAssign, newvalue, value, index, matchnum);
				index += matchnum;
			}

			result->colattribute.insert({column, prob->colattribute[column]});
			result->data.insert({column, newvalue});
			result->colorder.push_back(column);
		}
		for(unsigned j = 0; j < dataflow->colorder.size(); j++){
			std::string column = dataflow->colorder[j];
			
			if(std::find(usefulcolumn.begin(), usefulcolumn.end(), column) == usefulcolumn.end())
				continue;

			int toAssign[probsize];
			memset(toAssign, 0, probsize*sizeof(int));
			int matchnum = 0;

			for(int i = 0; i < probsize; i++){
				if(rgroupId[i] != 0)
					toAssign[matchnum++] = i;
			}

			int tgroupId[probsize];
			for(int i = 0; i < probsize; i++)
				tgroupId[i] = rgroupId[i];

			Integer* value = (Integer*)hashTable.values[j];
			Integer* newvalue = new Integer[count];
			int index = 0;
			gather(prob->select, toAssign, newvalue, value, tgroupId, index, matchnum);
			index += matchnum;

			while(matchnum > 0){
				matchnum = selectMisses(toAssign, tgroupId, sgroupId, matchnum);
				findNext(toAssign, sgroupId, tgroupId, matchnum);
				gather(prob->select, toAssign, newvalue, value, tgroupId, index, matchnum);
				index += matchnum;
			}

			result->colattribute.insert({column, dataflow->colattribute[column]});
			result->data.insert({column, newvalue});
			result->colorder.push_back(column);
		}

		delete [] sgroupId;
		return result;
	}
};

class PrintOperation : public Operation{
public:
	std::vector<std::string> columns;
	virtual Dataflow *execute(){
		int ss = 0;
		while(!end){
			Dataflow *dataflow = left->execute();
			print(dataflow, columns);
			end = left->end;
			ss+=dataflow->size;

			delete dataflow;
		}

std::cout << ss<<std::endl;

		return nullptr;
	}
	void print(Dataflow *dataflow, std::vector<std::string> &columns){
		for(int i = 0; i < dataflow->size; i++){
			for(std::string column : columns){
				const Schema::Relation::Attribute &attribute = dataflow->colattribute[column];
				switch(attribute.type){
				case Types::Tag::Integer:
					std::cout << *((Integer*)dataflow->data[column]+dataflow->select[i]) << " ";
					break;
				case Types::Tag::Numeric:
					switch(attribute.len2){
						case 1:
						std::cout << *((Numeric<LEN, 1>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 2:
						std::cout << *((Numeric<LEN, 2>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 3:
						std::cout << *((Numeric<LEN, 3>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 4:
						std::cout << *((Numeric<LEN, 4>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 5:
						std::cout << *((Numeric<LEN, 5>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 6:
						std::cout << *((Numeric<LEN, 6>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 7:
						std::cout << *((Numeric<LEN, 7>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 8:
						std::cout << *((Numeric<LEN, 8>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 9:
						std::cout << *((Numeric<LEN, 9>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 10:
						std::cout << *((Numeric<LEN, 10>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 11:
						std::cout << *((Numeric<LEN, 11>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 12:
						std::cout << *((Numeric<LEN, 12>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 13:
						std::cout << *((Numeric<LEN, 13>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 14:
						std::cout << *((Numeric<LEN, 14>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 15:
						std::cout << *((Numeric<LEN, 15>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 16:
						std::cout << *((Numeric<LEN, 16>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 17:
						std::cout << *((Numeric<LEN, 17>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						case 18:
						std::cout << *((Numeric<LEN, 18>*)dataflow->data[column]+dataflow->select[i]) << " ";
							break;
						default:
							break;
					}
					break;
				default:
					break;
				}
			}
			std::cout << std::endl;
		}
	}
};

