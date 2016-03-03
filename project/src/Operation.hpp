#ifndef HPP_OPERATION
#define HPP_OPERATION

#include <tuple>
#include <unordered_map>
#include <iostream>
#include <memory>
#include "./parser/Schema.hpp"
#include "project.hpp"

#include <vector>
#include <string>

constexpr int LEN = 32;
constexpr int N = 100000;

struct Dataflow;
using DataFlow  = std::shared_ptr<Dataflow>;

struct Dataflow{
	Dataflow(){}
	std::unordered_map<std::string, void*> data;
	std::vector<std::string> colorder;
	std::unordered_map<std::string, Schema::Relation::Attribute> colattribute;

	Dataflow(const Dataflow &other) = delete;
	Dataflow &operator = (const Dataflow &other) = delete;

	Dataflow(Dataflow &&other){
		size = other.size;
		flag = other.flag;
		name = other.name;
		select = new int[size];
		for(int i = 0; i < size; i++){
			select[i] = other.select[i];
		}
		colorder = other.colorder;
		colattribute = other.colattribute;
		for(std::string col : colorder){
			data.insert({col, other.data[col]});
			other.data[col] = nullptr;
		}
	}

	Dataflow &operator = (Dataflow &&other){
		size = other.size;
		flag = other.flag;
		name = other.name;
		select = new int[size];
		for(int i = 0; i < size; i++){
			select[i] = other.select[i];
		}
		colorder = other.colorder;
		colattribute = other.colattribute;
		for(std::string col : colorder){
			data.insert({col, other.data[col]});
			other.data[col] = nullptr;
		}
		return *this;
	}

	~Dataflow(){
		delete [] select;
//shared data if it's not join
		if(name != "join")
			return;
		for(std::string col : colorder){
			delete [] static_cast<Integer*>(data[col]);
		}
	}	
	
	int size = 0;
	int *select = nullptr;
	bool flag = false;
	std::string name = "";
};

class Operation{
public:
	std::string name;
	Operation *left = nullptr;
	Operation *right = nullptr;
	virtual DataFlow execute() = 0;
	virtual void refresh(){
	
	}
	bool end = false;
	std::vector<std::string> usefulcolumn;
	void push_back(std::string column);

	virtual ~Operation(){
		delete left;
		delete right;
	}
};

class ScanOperation : public Operation{
//scan the only needed columns
public:
	int offset = 0;
	std::vector<std::string> columns;
	std::unordered_map<std::string, Schema::Relation::Attribute> attributes;
	std::unordered_map<std::string, Table*> columnTable;

	virtual void refresh(){
		offset = 0;
		end = false;
	}
	virtual DataFlow execute(){
		return scan(columns);
	}
	
	DataFlow scan(const std::vector<std::string> &columns);
};

class SelectOperation : public Operation{
public:
	std::vector<std::tuple<std::string, std::string, std::string>> expression;
	std::unordered_map<std::string, Schema::Relation::Attribute> attributes;

	virtual void refresh(){
		left->refresh();
	}
	virtual DataFlow execute(){
		DataFlow dataflow = left->execute();
		end = left->end;
		return select(dataflow, expression);
	}
	DataFlow select(DataFlow &dataflow, const std::vector<std::tuple<std::string, std::string, std::string>> &expression);
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

	~HashTable(){
		for(unsigned i = 0; i < colorder.size(); i++){
			delete [] (Integer*)values[i];
		}
		delete [] first;
		delete [] next;
	}
	
};

class JoinOperation : public Operation{
public:
	bool built = false;
	bool newPhase = true;
	HashTable hashTable;
	DataFlow leftData = nullptr;
	DataFlow rightData = nullptr;
	unsigned M = 0;

	int sum = 0;
	int rightsum = 0;

	std::vector<std::tuple<std::string, std::string>> expression;
	virtual DataFlow execute();
	DataFlow hashjoin(const DataFlow &dataflow, const DataFlow &prob, const std::vector<std::tuple<std::string, std::string>> &expression);
};

class PrintOperation : public Operation{
public:
	std::vector<std::string> columns;
	virtual DataFlow execute(){
//		int count = 0;
		while(!end){
			DataFlow dataflow = left->execute();
			print(dataflow, columns);
			end = left->end;
//			count+=dataflow->size;

			
		}

//		std::cout <<"Number of results: "<< count <<std::endl;

		return nullptr;
	}
	void print(DataFlow dataflow, std::vector<std::string> &columns);
};
#endif
