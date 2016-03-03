#include "Operation.hpp"

int scan_column(Table *table, void** result, std::string column, int size, int offset){
	return table->getColumn(result, column, size, offset);
}

DataFlow ScanOperation::scan(const std::vector<std::string> &columns){
	DataFlow dataflow = std::make_shared<Dataflow>();
	int remain = 0;
	dataflow->select = new int[N];
	for(int i = 0; i < N; i++){
		dataflow->select[i]=i;
	}
	
	for(const std::string &column : columns){	
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

int select_equal_Integer(int *select, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(input[select[i]] == value){
			select[count++] = select[i];
		}
	}
	return count;
}

int select_bigger_Integer(int *select, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(input[select[i]] > value){
			select[count++] = select[i];
		}
	}
	return count;
}

int select_smaller_Integer(int *select, Integer *input, int value, int size){
	int count = 0;
	for(int i = 0; i < size; i++){
		if(input[select[i]] < value){
			select[count++] = select[i];
		}
	}
	return count;
}

DataFlow SelectOperation::select(DataFlow &dataflow, const std::vector<std::tuple<std::string, std::string, std::string>> &expression){
	int size = dataflow->size;
	for(const std::tuple<std::string, std::string, std::string> &exp : expression){
		if(std::get<0>(exp) == "="){
			switch(attributes[std::get<1>(exp)].type){
				case Types::Tag::Integer:
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

void Operation::push_back(std::string column){
	usefulcolumn.push_back(column);
	Operation *t = left;
	while(t != nullptr){
		t->usefulcolumn.push_back(column);
		t = t->left;
	}
}

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
		toCheck[count] = i;
		count += (first[hashValue[i]] != 0);
		pgroupId[i] = first[hashValue[i]];

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

DataFlow JoinOperation::execute(){

	if(right->end){
		right->refresh();
		newPhase = true;
		built = false;
		M = 0;
	}

	if(newPhase){
		newPhase = false;

		leftData = left->execute();
		sum += leftData->size;
		if(leftData->size == 0){
			end = true;
			return std::make_shared<Dataflow>();
		}
	}

	rightData = right->execute();
	rightsum += rightData->size;

	if(left->end && right->end){
		end = true;	
	}

	if(rightData->size == 0)
		return std::make_shared<Dataflow>();
	return hashjoin(leftData, rightData, expression);
}

DataFlow JoinOperation::hashjoin(const DataFlow &dataflow, const DataFlow &prob, const std::vector<std::tuple<std::string, std::string>> &expression){
	if(!built){
		built = true;
		delete [] hashTable.first;
		hashTable.first = new int[dataflow->size];
		delete [] hashTable.next;
		hashTable.next = new int[dataflow->size+1];
		memset(hashTable.first, 0, dataflow->size*sizeof(int));
		memset(hashTable.next, 0, (dataflow->size+1)*sizeof(int));
		int *hashValue = new int[dataflow->size];

		memset(hashValue, 0, dataflow->size*sizeof(int));

//hashvalue of each row is the sum of each key
		for(const std::tuple<std::string, std::string> &exp: expression){
			hash(dataflow->select, hashValue, (Integer*)dataflow->data[std::get<0>(exp)], dataflow->size);
		}

		bucket(hashValue, dataflow->size, dataflow->size);
		int *groupId = new int[dataflow->size];

//build the hashtable
		hashTableInsert(dataflow->select, groupId, hashTable, hashValue, dataflow->size);
		delete [] hashValue;

//insert columns which are in join condition
		for(const std::tuple<std::string, std::string> &exp: expression){
			delete [] (Integer*)hashTable.values[M];

			hashTable.values[M] = new Integer[dataflow->size+1];	
			hashTable.colorder.push_back(std::get<0>(exp));

			spread_Integer(dataflow->select, (Integer*)hashTable.values[M], groupId, (Integer*)dataflow->data[std::get<0>(exp)], dataflow->size);
			M++;
		}

//insert columns which are not in join condition
		for(const std::string &column : dataflow->colorder){
			if(std::find(hashTable.colorder.begin(), hashTable.colorder.end(),column) != hashTable.colorder.end())
				continue;

			if(hashTable.values[M] != nullptr)
				delete [] (Integer*)hashTable.values[M];

			hashTable.values[M] = new Integer[dataflow->size+1];
			hashTable.colorder.push_back(column);

			spread_Integer(dataflow->select, (Integer*)hashTable.values[M], groupId, (Integer*)dataflow->data[column], dataflow->size);
			M++;
		}
		delete [] groupId;
	}

	int probsize = prob->size;
	DataFlow result = std::make_shared<Dataflow>();;
	result->name = "join";
	result->flag = true;
	if(probsize == 0){
		return result;
	}
	int probehashValue[probsize];
	memset(probehashValue, 0, probsize*sizeof(int));

	for(const std::tuple<std::string, std::string> &exp : expression){
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

//look up the first match for probe table and store in pgroupId
//If the value is not 0, it means it is a real match
	int m =	lookupInitial(toCheck, pgroupId, hashTable.first, probehashValue, probsize);
	while(m > 0){
		int differ[probsize];
		memset(differ, 0, probsize*sizeof(int));
		int i = 0;
		for(const std::tuple<std::string, std::string> &exp: expression){
//check if it is a real match
			check(prob->select, differ, toCheck, pgroupId, rgroupId, sgroupId, (Integer*)hashTable.values[i], (Integer*)prob->data[std::get<1>(exp)], m);
			i++;
		}
//build the matching table for probe table and store in rgroupId, sgroupId
		build(differ, toCheck, pgroupId, rgroupId, sgroupId, m);
		m = selectMisses(toCheck, pgroupId, hashTable.next, m);
//m = 0 means there is no more matches
//look up the next match
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
//check if the column will be needed in parent operators
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
		
//put each row of probe table to proper position in final result
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
//put each row of build table to proper position in final result
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

void print_Integer(int *select, Integer *value, int size){
	for(int i = 0; i < size; i++){
		std::cout << value[select[i]] << " ";
	}
}

void PrintOperation::print(DataFlow dataflow, std::vector<std::string> &columns){
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
