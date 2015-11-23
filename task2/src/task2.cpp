/*
 * stock.cpp
 *
 *  Created on: Oct 24, 2015
 *      Author: spiritlcx
 */

#include <tuple>
#include <utility>
#include <unordered_map>
#include <string>
#include <vector>
#include <iostream>
#include "Types.hpp"
#include <fstream>
#include <list>
#include <cstdio>
#include <ctime>
#include "parser/Parser.hpp"
namespace Nwarehouse{
	typedef std::tuple<Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)
			);
		}
	};
struct warehouse{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer w_id;
		Varchar<10> w_name;
		Varchar<20> w_street_1;
		Varchar<20> w_street_2;
		Varchar<20> w_city;
		Char<2> w_state;
		Char<9> w_zip;
		Numeric<4,4> w_tax;
		Numeric<12,2> w_ytd;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> warehouses;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(0);

		attributes.push_back(Attribute("w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("w_name",Types::Tag::Varchar, 10, 0, 0));
		attributes.push_back(Attribute("w_street_1",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("w_street_2",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("w_city",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("w_state",Types::Tag::Char, 2, 0, 0));
		attributes.push_back(Attribute("w_zip",Types::Tag::Char, 9, 0, 0));
		attributes.push_back(Attribute("w_tax",Types::Tag::Numeric, 4, 4, 0));
		attributes.push_back(Attribute("w_ytd",Types::Tag::Numeric, 12, 2, 0));

		std::string path = "data/tpcc_warehouse.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.w_id =Integer::castString(data[0].c_str(), data[0].length());
			row.w_name =Varchar<10>::castString(data[1].c_str(), data[1].length());
			row.w_street_1 =Varchar<20>::castString(data[2].c_str(), data[2].length());
			row.w_street_2 =Varchar<20>::castString(data[3].c_str(), data[3].length());
			row.w_city =Varchar<20>::castString(data[4].c_str(), data[4].length());
			row.w_state =Char<2>::castString(data[5].c_str(), data[5].length());
			row.w_zip =Char<9>::castString(data[6].c_str(), data[6].length());
			row.w_tax =Numeric<4,4>::castString(data[7].c_str(), data[7].length());
			row.w_ytd =Numeric<12,2>::castString(data[8].c_str(), data[8].length());
			warehouses.push_back(row);

			m[std::make_tuple(row.w_id)] = row;
		}
	}
	Row *find(std::tuple<Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer w_id,Varchar<10> w_name,Varchar<20> w_street_1,Varchar<20> w_street_2,Varchar<20> w_city,Char<2> w_state,Char<9> w_zip,Numeric<4,4> w_tax,Numeric<12,2> w_ytd){
			Row row;
			row.w_id = w_id;
			row.w_name = w_name;
			row.w_street_1 = w_street_1;
			row.w_street_2 = w_street_2;
			row.w_city = w_city;
			row.w_state = w_state;
			row.w_zip = w_zip;
			row.w_tax = w_tax;
			row.w_ytd = w_ytd;
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Ndistrict{
	typedef std::tuple<Integer,Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value+std::get<1>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)&&
				std::get<1>(v0) == std::get<1>(v1)
			);
		}
	};
struct district{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer d_id;
		Integer d_w_id;
		Varchar<10> d_name;
		Varchar<20> d_street_1;
		Varchar<20> d_street_2;
		Varchar<20> d_city;
		Char<2> d_state;
		Char<9> d_zip;
		Numeric<4,4> d_tax;
		Numeric<12,2> d_ytd;
		Integer d_next_o_id;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> districts;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(1);
		primaryKey.push_back(0);

		attributes.push_back(Attribute("d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("d_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("d_name",Types::Tag::Varchar, 10, 0, 0));
		attributes.push_back(Attribute("d_street_1",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("d_street_2",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("d_city",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("d_state",Types::Tag::Char, 2, 0, 0));
		attributes.push_back(Attribute("d_zip",Types::Tag::Char, 9, 0, 0));
		attributes.push_back(Attribute("d_tax",Types::Tag::Numeric, 4, 4, 0));
		attributes.push_back(Attribute("d_ytd",Types::Tag::Numeric, 12, 2, 0));
		attributes.push_back(Attribute("d_next_o_id",Types::Tag::Integer, 0, 0, 0));

		std::string path = "data/tpcc_district.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.d_id =Integer::castString(data[0].c_str(), data[0].length());
			row.d_w_id =Integer::castString(data[1].c_str(), data[1].length());
			row.d_name =Varchar<10>::castString(data[2].c_str(), data[2].length());
			row.d_street_1 =Varchar<20>::castString(data[3].c_str(), data[3].length());
			row.d_street_2 =Varchar<20>::castString(data[4].c_str(), data[4].length());
			row.d_city =Varchar<20>::castString(data[5].c_str(), data[5].length());
			row.d_state =Char<2>::castString(data[6].c_str(), data[6].length());
			row.d_zip =Char<9>::castString(data[7].c_str(), data[7].length());
			row.d_tax =Numeric<4,4>::castString(data[8].c_str(), data[8].length());
			row.d_ytd =Numeric<12,2>::castString(data[9].c_str(), data[9].length());
			row.d_next_o_id =Integer::castString(data[10].c_str(), data[10].length());
			districts.push_back(row);

			m[std::make_tuple(row.d_w_id,row.d_id)] = row;
		}
	}
	Row *find(std::tuple<Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer d_id,Integer d_w_id,Varchar<10> d_name,Varchar<20> d_street_1,Varchar<20> d_street_2,Varchar<20> d_city,Char<2> d_state,Char<9> d_zip,Numeric<4,4> d_tax,Numeric<12,2> d_ytd,Integer d_next_o_id){
			Row row;
			row.d_id = d_id;
			row.d_w_id = d_w_id;
			row.d_name = d_name;
			row.d_street_1 = d_street_1;
			row.d_street_2 = d_street_2;
			row.d_city = d_city;
			row.d_state = d_state;
			row.d_zip = d_zip;
			row.d_tax = d_tax;
			row.d_ytd = d_ytd;
			row.d_next_o_id = d_next_o_id;
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Ncustomer{
	typedef std::tuple<Integer,Integer,Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)&&
				std::get<1>(v0) == std::get<1>(v1)&&
				std::get<2>(v0) == std::get<2>(v1)
			);
		}
	};
struct customer{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer c_id;
		Integer c_d_id;
		Integer c_w_id;
		Varchar<16> c_first;
		Char<2> c_middle;
		Varchar<16> c_last;
		Varchar<20> c_street_1;
		Varchar<20> c_street_2;
		Varchar<20> c_city;
		Char<2> c_state;
		Char<9> c_zip;
		Char<16> c_phone;
		Timestamp c_since;
		Char<2> c_credit;
		Numeric<12,2> c_credit_lim;
		Numeric<4,4> c_discount;
		Numeric<12,2> c_balance;
		Numeric<12,2> c_ytd_paymenr;
		Numeric<4,0> c_payment_cnt;
		Numeric<4,0> c_delivery_cnt;
		Varchar<500> c_data;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> customers;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(2);
		primaryKey.push_back(1);
		primaryKey.push_back(0);

		attributes.push_back(Attribute("c_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("c_d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("c_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("c_first",Types::Tag::Varchar, 16, 0, 0));
		attributes.push_back(Attribute("c_middle",Types::Tag::Char, 2, 0, 0));
		attributes.push_back(Attribute("c_last",Types::Tag::Varchar, 16, 0, 0));
		attributes.push_back(Attribute("c_street_1",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("c_street_2",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("c_city",Types::Tag::Varchar, 20, 0, 0));
		attributes.push_back(Attribute("c_state",Types::Tag::Char, 2, 0, 0));
		attributes.push_back(Attribute("c_zip",Types::Tag::Char, 9, 0, 0));
		attributes.push_back(Attribute("c_phone",Types::Tag::Char, 16, 0, 0));
		attributes.push_back(Attribute("c_since",Types::Tag::Timestamp, 0, 0, 0));
		attributes.push_back(Attribute("c_credit",Types::Tag::Char, 2, 0, 0));
		attributes.push_back(Attribute("c_credit_lim",Types::Tag::Numeric, 12, 2, 0));
		attributes.push_back(Attribute("c_discount",Types::Tag::Numeric, 4, 4, 0));
		attributes.push_back(Attribute("c_balance",Types::Tag::Numeric, 12, 2, 0));
		attributes.push_back(Attribute("c_ytd_paymenr",Types::Tag::Numeric, 12, 2, 0));
		attributes.push_back(Attribute("c_payment_cnt",Types::Tag::Numeric, 4, 0, 0));
		attributes.push_back(Attribute("c_delivery_cnt",Types::Tag::Numeric, 4, 0, 0));
		attributes.push_back(Attribute("c_data",Types::Tag::Varchar, 500, 0, 0));

		std::string path = "data/tpcc_customer.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.c_id =Integer::castString(data[0].c_str(), data[0].length());
			row.c_d_id =Integer::castString(data[1].c_str(), data[1].length());
			row.c_w_id =Integer::castString(data[2].c_str(), data[2].length());
			row.c_first =Varchar<16>::castString(data[3].c_str(), data[3].length());
			row.c_middle =Char<2>::castString(data[4].c_str(), data[4].length());
			row.c_last =Varchar<16>::castString(data[5].c_str(), data[5].length());
			row.c_street_1 =Varchar<20>::castString(data[6].c_str(), data[6].length());
			row.c_street_2 =Varchar<20>::castString(data[7].c_str(), data[7].length());
			row.c_city =Varchar<20>::castString(data[8].c_str(), data[8].length());
			row.c_state =Char<2>::castString(data[9].c_str(), data[9].length());
			row.c_zip =Char<9>::castString(data[10].c_str(), data[10].length());
			row.c_phone =Char<16>::castString(data[11].c_str(), data[11].length());
//			row.c_since =Timestamp::castString(data[12].c_str(), data[12].length());
			row.c_credit =Char<2>::castString(data[13].c_str(), data[13].length());
			row.c_credit_lim =Numeric<12,2>::castString(data[14].c_str(), data[14].length());
			row.c_discount =Numeric<4,4>::castString(data[15].c_str(), data[15].length());
			row.c_balance =Numeric<12,2>::castString(data[16].c_str(), data[16].length());
			row.c_ytd_paymenr =Numeric<12,2>::castString(data[17].c_str(), data[17].length());
			row.c_payment_cnt =Numeric<4,0>::castString(data[18].c_str(), data[18].length());
			row.c_delivery_cnt =Numeric<4,0>::castString(data[19].c_str(), data[19].length());
			row.c_data =Varchar<500>::castString(data[20].c_str(), data[20].length());
			customers.push_back(row);

			m[std::make_tuple(row.c_w_id,row.c_d_id,row.c_id)] = row;
		}
	}
	Row *find(std::tuple<Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer c_id,Integer c_d_id,Integer c_w_id,Varchar<16> c_first,Char<2> c_middle,Varchar<16> c_last,Varchar<20> c_street_1,Varchar<20> c_street_2,Varchar<20> c_city,Char<2> c_state,Char<9> c_zip,Char<16> c_phone,Timestamp c_since,Char<2> c_credit,Numeric<12,2> c_credit_lim,Numeric<4,4> c_discount,Numeric<12,2> c_balance,Numeric<12,2> c_ytd_paymenr,Numeric<4,0> c_payment_cnt,Numeric<4,0> c_delivery_cnt,Varchar<500> c_data){
			Row row;
			row.c_id = c_id;
			row.c_d_id = c_d_id;
			row.c_w_id = c_w_id;
			row.c_first = c_first;
			row.c_middle = c_middle;
			row.c_last = c_last;
			row.c_street_1 = c_street_1;
			row.c_street_2 = c_street_2;
			row.c_city = c_city;
			row.c_state = c_state;
			row.c_zip = c_zip;
			row.c_phone = c_phone;
			row.c_since = c_since;
			row.c_credit = c_credit;
			row.c_credit_lim = c_credit_lim;
			row.c_discount = c_discount;
			row.c_balance = c_balance;
			row.c_ytd_paymenr = c_ytd_paymenr;
			row.c_payment_cnt = c_payment_cnt;
			row.c_delivery_cnt = c_delivery_cnt;
			row.c_data = c_data;
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Nhistory{
struct history{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer h_c_id;
		Integer h_c_d_id;
		Integer h_c_w_id;
		Integer h_d_id;
		Integer h_w_id;
		Timestamp h_date;
		Numeric<6,2> h_amount;
		Varchar<24> h_data;
	};
	std::list<Row> historys;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	void init(){

		attributes.push_back(Attribute("h_c_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("h_c_d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("h_c_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("h_d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("h_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("h_date",Types::Tag::Timestamp, 0, 0, 0));
		attributes.push_back(Attribute("h_amount",Types::Tag::Numeric, 6, 2, 0));
		attributes.push_back(Attribute("h_data",Types::Tag::Varchar, 24, 0, 0));

		std::string path = "data/tpcc_history.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.h_c_id =Integer::castString(data[0].c_str(), data[0].length());
			row.h_c_d_id =Integer::castString(data[1].c_str(), data[1].length());
			row.h_c_w_id =Integer::castString(data[2].c_str(), data[2].length());
			row.h_d_id =Integer::castString(data[3].c_str(), data[3].length());
			row.h_w_id =Integer::castString(data[4].c_str(), data[4].length());
			row.h_date =Timestamp::castString(data[5].c_str(), data[5].length());
			row.h_amount =Numeric<6,2>::castString(data[6].c_str(), data[6].length());
			row.h_data =Varchar<24>::castString(data[7].c_str(), data[7].length());
			historys.push_back(row);

		}
	}
	void insert(Integer h_c_id,Integer h_c_d_id,Integer h_c_w_id,Integer h_d_id,Integer h_w_id,Timestamp h_date,Numeric<6,2> h_amount,Varchar<24> h_data){
			Row row;
			row.h_c_id = h_c_id;
			row.h_c_d_id = h_c_d_id;
			row.h_c_w_id = h_c_w_id;
			row.h_d_id = h_d_id;
			row.h_w_id = h_w_id;
			row.h_date = h_date;
			row.h_amount = h_amount;
			row.h_data = h_data;
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Nneworder{
	typedef std::tuple<Integer,Integer,Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)&&
				std::get<1>(v0) == std::get<1>(v1)&&
				std::get<2>(v0) == std::get<2>(v1)
			);
		}
	};
struct neworder{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer no_o_id;
		Integer no_d_id;
		Integer no_w_id;
		bool operator == (const Row &other) const {
			return this->no_d_id == other.no_d_id && this->no_o_id == other.no_o_id && this->no_w_id==other.no_w_id;
		}
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> neworders;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(2);
		primaryKey.push_back(1);
		primaryKey.push_back(0);

		attributes.push_back(Attribute("no_o_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("no_d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("no_w_id",Types::Tag::Integer, 0, 0, 0));

		std::string path = "data/tpcc_neworder.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.no_o_id =Integer::castString(data[0].c_str(), data[0].length());
			row.no_d_id =Integer::castString(data[1].c_str(), data[1].length());
			row.no_w_id =Integer::castString(data[2].c_str(), data[2].length());
			neworders.push_back(row);

			m[std::make_tuple(row.no_w_id,row.no_d_id,row.no_o_id)] = row;
		}
	}
	Row *find(std::tuple<Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void remove(Row row){
		neworders.remove(row);
////		m[std::make_tuple(row->no_w_id, row->no_d_id, row->no_o_id)] = nullptr_t;
	}
	void insert(Integer no_o_id,Integer no_d_id,Integer no_w_id){
			Row row;
			row.no_o_id = no_o_id;
			row.no_d_id = no_d_id;
			row.no_w_id = no_w_id;
			neworders.push_back(row);
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Norder{
	typedef std::tuple<Integer,Integer,Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)&&
				std::get<1>(v0) == std::get<1>(v1)&&
				std::get<2>(v0) == std::get<2>(v1)
			);
		}
	};
struct order{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer o_id;
		Integer o_d_id;
		Integer o_w_id;
		Integer o_c_id;
		Timestamp o_entry_d;
		Integer o_carrier_id;
		Numeric<2,0> o_ol_cnt;
		Numeric<1,0> o_all_local;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> orders;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(2);
		primaryKey.push_back(1);
		primaryKey.push_back(0);

		attributes.push_back(Attribute("o_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("o_d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("o_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("o_c_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("o_entry_d",Types::Tag::Timestamp, 0, 0, 0));
		attributes.push_back(Attribute("o_carrier_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("o_ol_cnt",Types::Tag::Numeric, 2, 0, 0));
		attributes.push_back(Attribute("o_all_local",Types::Tag::Numeric, 1, 0, 0));

		std::string path = "data/tpcc_order.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.o_id =Integer::castString(data[0].c_str(), data[0].length());
			row.o_d_id =Integer::castString(data[1].c_str(), data[1].length());
			row.o_w_id =Integer::castString(data[2].c_str(), data[2].length());
			row.o_c_id =Integer::castString(data[3].c_str(), data[3].length());
//			row.o_entry_d =Timestamp::castString(data[4].c_str(), data[4].length());
			row.o_carrier_id =Integer::castString(data[5].c_str(), data[5].length());
			row.o_ol_cnt =Numeric<2,0>::castString(data[6].c_str(), data[6].length());
			row.o_all_local =Numeric<1,0>::castString(data[7].c_str(), data[7].length());
			orders.push_back(row);

			m[std::make_tuple(row.o_w_id,row.o_d_id,row.o_id)] = row;
		}
	}
	Row *find(std::tuple<Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer o_id,Integer o_d_id,Integer o_w_id,Integer o_c_id,Timestamp o_entry_d,Integer o_carrier_id,Numeric<2,0> o_ol_cnt,Numeric<1,0> o_all_local){
			Row row;
			row.o_id = o_id;
			row.o_d_id = o_d_id;
			row.o_w_id = o_w_id;
			row.o_c_id = o_c_id;
			row.o_entry_d = o_entry_d;
			row.o_carrier_id = o_carrier_id;
			row.o_ol_cnt = o_ol_cnt;
			row.o_all_local = o_all_local;
			orders.push_back(row);
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Norderline{
	typedef std::tuple<Integer,Integer,Integer,Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value+std::get<3>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)&&
				std::get<1>(v0) == std::get<1>(v1)&&
				std::get<2>(v0) == std::get<2>(v1)&&
				std::get<3>(v0) == std::get<3>(v1)
			);
		}
	};
struct orderline{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer ol_o_id;
		Integer ol_d_id;
		Integer ol_w_id;
		Integer ol_number;
		Integer ol_i_id;
		Integer ol_supply_w_id;
		Timestamp ol_delivery_d;
		Numeric<2,0> ol_quantity;
		Numeric<6,2> ol_amount;
		Char<24> ol_dist_info;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> orderlines;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(2);
		primaryKey.push_back(1);
		primaryKey.push_back(0);
		primaryKey.push_back(3);

		attributes.push_back(Attribute("ol_o_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("ol_d_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("ol_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("ol_number",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("ol_i_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("ol_supply_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("ol_delivery_d",Types::Tag::Timestamp, 0, 0, 0));
		attributes.push_back(Attribute("ol_quantity",Types::Tag::Numeric, 2, 0, 0));
		attributes.push_back(Attribute("ol_amount",Types::Tag::Numeric, 6, 2, 0));
		attributes.push_back(Attribute("ol_dist_info",Types::Tag::Char, 24, 0, 0));

		std::string path = "data/tpcc_orderline.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.ol_o_id =Integer::castString(data[0].c_str(), data[0].length());
			row.ol_d_id =Integer::castString(data[1].c_str(), data[1].length());
			row.ol_w_id =Integer::castString(data[2].c_str(), data[2].length());
			row.ol_number =Integer::castString(data[3].c_str(), data[3].length());
			row.ol_i_id =Integer::castString(data[4].c_str(), data[4].length());
			row.ol_supply_w_id =Integer::castString(data[5].c_str(), data[5].length());
//			row.ol_delivery_d =Timestamp::castString(data[6].c_str(), data[6].length());
			row.ol_quantity =Numeric<2,0>::castString(data[7].c_str(), data[7].length());
			row.ol_amount =Numeric<6,2>::castString(data[8].c_str(), data[8].length());
			row.ol_dist_info =Char<24>::castString(data[9].c_str(), data[9].length());
			orderlines.push_back(row);

			m[std::make_tuple(row.ol_w_id,row.ol_d_id,row.ol_o_id,row.ol_number)] = row;
		}
	}
	Row *find(std::tuple<Integer,Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer ol_o_id,Integer ol_d_id,Integer ol_w_id,Integer ol_number,Integer ol_i_id,Integer ol_supply_w_id,Timestamp ol_delivery_d,Numeric<2,0> ol_quantity,Numeric<6,2> ol_amount,Char<24> ol_dist_info){
			Row row;
			row.ol_o_id = ol_o_id;
			row.ol_d_id = ol_d_id;
			row.ol_w_id = ol_w_id;
			row.ol_number = ol_number;
			row.ol_i_id = ol_i_id;
			row.ol_supply_w_id = ol_supply_w_id;
			row.ol_delivery_d = ol_delivery_d;
			row.ol_quantity = ol_quantity;
			row.ol_amount = ol_amount;
			row.ol_dist_info = ol_dist_info;
			orderlines.push_back(row);
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Nitem{
	typedef std::tuple<Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)
			);
		}
	};
struct item{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer i_id;
		Integer i_im_id;
		Varchar<24> i_name;
		Numeric<5,2> i_price;
		Varchar<50> i_data;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> items;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(0);

		attributes.push_back(Attribute("i_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("i_im_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("i_name",Types::Tag::Varchar, 24, 0, 0));
		attributes.push_back(Attribute("i_price",Types::Tag::Numeric, 5, 2, 0));
		attributes.push_back(Attribute("i_data",Types::Tag::Varchar, 50, 0, 0));

		std::string path = "data/tpcc_item.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.i_id =Integer::castString(data[0].c_str(), data[0].length());
			row.i_im_id =Integer::castString(data[1].c_str(), data[1].length());
			row.i_name =Varchar<24>::castString(data[2].c_str(), data[2].length());
			row.i_price =Numeric<5,2>::castString(data[3].c_str(), data[3].length());
			row.i_data =Varchar<50>::castString(data[4].c_str(), data[4].length());
			items.push_back(row);

			m[std::make_tuple(row.i_id)] = row;
		}
	}
	Row *find(std::tuple<Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer i_id,Integer i_im_id,Varchar<24> i_name,Numeric<5,2> i_price,Varchar<50> i_data){
			Row row;
			row.i_id = i_id;
			row.i_im_id = i_im_id;
			row.i_name = i_name;
			row.i_price = i_price;
			row.i_data = i_data;
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}
namespace Nstock{
	typedef std::tuple<Integer,Integer> key_t;
	struct key_hash : public std::unary_function<key_t, std::size_t>{
		std::size_t operator()(const key_t& k) const{
			return std::get<0>(k).value+std::get<1>(k).value;
		}
	};
	struct key_equal : public std::binary_function<key_t, key_t, bool>{
		bool operator()(const key_t& v0, const key_t& v1) const{
			return (
				std::get<0>(v0) == std::get<0>(v1)&&
				std::get<1>(v0) == std::get<1>(v1)
			);
		}
	};
struct stock{
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};
	struct Row{
		Integer s_i_id;
		Integer s_w_id;
		Numeric<4,0> s_quantity;
		Char<24> s_dist_01;
		Char<24> s_dist_02;
		Char<24> s_dist_03;
		Char<24> s_dist_04;
		Char<24> s_dist_05;
		Char<24> s_dist_06;
		Char<24> s_dist_07;
		Char<24> s_dist_08;
		Char<24> s_dist_09;
		Char<24> s_dist_10;
		Numeric<8,0> s_ytd;
		Numeric<4,0> s_order_cnt;
		Numeric<4,0> s_remote_cnt;
		Varchar<50> s_data;
	};
	typedef std::unordered_map<const key_t,Row,key_hash,key_equal> map_t;
	std::list<Row> stocks;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	void init(){
		primaryKey.push_back(1);
		primaryKey.push_back(0);

		attributes.push_back(Attribute("s_i_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("s_w_id",Types::Tag::Integer, 0, 0, 0));
		attributes.push_back(Attribute("s_quantity",Types::Tag::Numeric, 4, 0, 0));
		attributes.push_back(Attribute("s_dist_01",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_02",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_03",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_04",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_05",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_06",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_07",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_08",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_09",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_dist_10",Types::Tag::Char, 24, 0, 0));
		attributes.push_back(Attribute("s_ytd",Types::Tag::Numeric, 8, 0, 0));
		attributes.push_back(Attribute("s_order_cnt",Types::Tag::Numeric, 4, 0, 0));
		attributes.push_back(Attribute("s_remote_cnt",Types::Tag::Numeric, 4, 0, 0));
		attributes.push_back(Attribute("s_data",Types::Tag::Varchar, 50, 0, 0));

		std::string path = "data/tpcc_stock.tbl";
		std::ifstream f(path);
		std::string line;
		Parser parser;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = parser.split(line, '|');
			Row row;
			row.s_i_id =Integer::castString(data[0].c_str(), data[0].length());
			row.s_w_id =Integer::castString(data[1].c_str(), data[1].length());
			row.s_quantity =Numeric<4,0>::castString(data[2].c_str(), data[2].length());
			row.s_dist_01 =Char<24>::castString(data[3].c_str(), data[3].length());
			row.s_dist_02 =Char<24>::castString(data[4].c_str(), data[4].length());
			row.s_dist_03 =Char<24>::castString(data[5].c_str(), data[5].length());
			row.s_dist_04 =Char<24>::castString(data[6].c_str(), data[6].length());
			row.s_dist_05 =Char<24>::castString(data[7].c_str(), data[7].length());
			row.s_dist_06 =Char<24>::castString(data[8].c_str(), data[8].length());
			row.s_dist_07 =Char<24>::castString(data[9].c_str(), data[9].length());
			row.s_dist_08 =Char<24>::castString(data[10].c_str(), data[10].length());
			row.s_dist_09 =Char<24>::castString(data[11].c_str(), data[11].length());
			row.s_dist_10 =Char<24>::castString(data[12].c_str(), data[12].length());
			row.s_ytd =Numeric<8,0>::castString(data[13].c_str(), data[13].length());
			row.s_order_cnt =Numeric<4,0>::castString(data[14].c_str(), data[14].length());
			row.s_remote_cnt =Numeric<4,0>::castString(data[15].c_str(), data[15].length());
			row.s_data =Varchar<50>::castString(data[16].c_str(), data[16].length());
			stocks.push_back(row);

			m[std::make_tuple(row.s_w_id,row.s_i_id)] = row;
		}
	}
	Row *find(std::tuple<Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return &itr->second;
		}
		else{
			return nullptr;
		}
	}
	void insert(Integer s_i_id,Integer s_w_id,Numeric<4,0> s_quantity,Char<24> s_dist_01,Char<24> s_dist_02,Char<24> s_dist_03,Char<24> s_dist_04,Char<24> s_dist_05,Char<24> s_dist_06,Char<24> s_dist_07,Char<24> s_dist_08,Char<24> s_dist_09,Char<24> s_dist_10,Numeric<8,0> s_ytd,Numeric<4,0> s_order_cnt,Numeric<4,0> s_remote_cnt,Varchar<50> s_data){
			Row row;
			row.s_i_id = s_i_id;
			row.s_w_id = s_w_id;
			row.s_quantity = s_quantity;
			row.s_dist_01 = s_dist_01;
			row.s_dist_02 = s_dist_02;
			row.s_dist_03 = s_dist_03;
			row.s_dist_04 = s_dist_04;
			row.s_dist_05 = s_dist_05;
			row.s_dist_06 = s_dist_06;
			row.s_dist_07 = s_dist_07;
			row.s_dist_08 = s_dist_08;
			row.s_dist_09 = s_dist_09;
			row.s_dist_10 = s_dist_10;
			row.s_ytd = s_ytd;
			row.s_order_cnt = s_order_cnt;
			row.s_remote_cnt = s_remote_cnt;
			row.s_data = s_data;
}
	void insertAttribute(Attribute attribute){
		attributes.push_back(attribute);
	}
	void deleteAttribute(){
	}
	std::vector<Attribute> getAttributes(){
		return attributes;
	}
};
}


const int32_t warehouses=5;

int32_t urand(int32_t min,int32_t max) {
   return (random()%(max-min+1))+min;
}

int32_t urandexcept(int32_t min,int32_t max,int32_t v) {
   if (max<=min)
      return min;
   int32_t r=(random()%(max-min))+min;
   if (r>=v)
      return r+1; else
      return r;
}

int32_t nurand(int32_t A,int32_t x,int32_t y) {
   return ((((random()%A)|(random()%(y-x+1)+x))+42)%(y-x+1))+x;
}

void newOrder(int32_t w_id, int32_t d_id, int32_t c_id, int32_t items, int32_t *supware, int32_t *itemid, int32_t *qty, Timestamp datetime, Nwarehouse::warehouse &warehouse, Ncustomer::customer &customer, Ndistrict::district &district, Nstock::stock &stock, Nitem::item &item, Norder::order &order, Nneworder::neworder &neworder, Norderline::orderline &orderline){
	Nwarehouse::warehouse::Row *warehouserow= warehouse.find(std::make_tuple(Integer(w_id)));
	Numeric<4,4> w_tax = warehouserow->w_tax;
	Ncustomer::customer::Row *customerrow= customer.find(std::make_tuple(Integer(w_id), Integer(d_id), Integer(c_id)));
	Numeric<4,4> c_discount = customerrow->c_discount;
	Ndistrict::district::Row *districtrow = district.find(std::make_tuple(Integer(w_id), Integer(d_id)));
	Integer o_id = districtrow->d_next_o_id;
	Numeric<4,4> d_tax = districtrow->d_tax;
	districtrow->d_next_o_id = Integer(o_id.value + 1);

	Integer all_local = 1;
	for(int i = 0; i < items; i++){
		if(w_id != supware[i])
			all_local = 0;
	}

	order.insert(o_id, d_id, w_id, c_id, datetime, 0, items, all_local);
	neworder.insert(o_id, d_id, w_id);
	//insert into order
	//

	for(int i = 0; i < items; i++){
		Nitem::item::Row *itemrow = item.find(std::make_tuple(Integer(itemid[i])));
		Numeric<5, 2> iprice = itemrow->i_price;
		Nstock::stock::Row *stockrow = stock.find(std::make_tuple(Integer(supware[i]),Integer(itemid[i])));
		Numeric<4,0> s_quantity = stockrow->s_quantity;
		Numeric<4,0> s_remote = stockrow->s_remote_cnt;
		Numeric<4,0> s_order_cnt = stockrow->s_order_cnt;
		Char<24> s_dist;
		switch(d_id){
		case 1:
			s_dist = stockrow->s_dist_01;
			break;
		case 2:
			s_dist = stockrow->s_dist_02;
			break;
		case 3:
			s_dist = stockrow->s_dist_03;
			break;
		case 4:
			s_dist = stockrow->s_dist_04;
			break;
		case 5:
			s_dist = stockrow->s_dist_05;
			break;
		case 6:
			s_dist = stockrow->s_dist_06;
			break;
		case 7:
			s_dist = stockrow->s_dist_07;
			break;
		case 8:
			s_dist = stockrow->s_dist_08;
			break;
		case 9:
			s_dist = stockrow->s_dist_09;
			break;
		case 10:
			s_dist = stockrow->s_dist_10;
			break;

		}

		stockrow = stock.find(std::make_tuple(Integer(supware[i]), Integer(itemid[i])));
		if(s_quantity > qty[i]){
			stockrow->s_quantity = stockrow->s_quantity - qty[i];
		}else{
			stockrow->s_quantity = stockrow->s_quantity + 91 - qty[i];
		}

		stockrow = stock.find(std::make_tuple(Integer(w_id), Integer(itemid[i])));
		if(supware[i] != w_id){
			stockrow->s_remote_cnt += 1;
		}else{
			stockrow->s_order_cnt += 1;
		}

		Numeric<6,2> ol_amount = qty[i]*iprice.value*(1+w_tax.value+d_tax.value)*(1-c_discount.value);
		orderline.insert(o_id,d_id,w_id,i+1,itemid[i],supware[i],0,qty[i],ol_amount,s_dist);
	}

}

void newOrderRandom(Timestamp now,int32_t w_id, Nwarehouse::warehouse &warehouse, Ncustomer::customer &customer, Ndistrict::district &district, Nstock::stock &stock, Nitem::item &item, Norder::order &order, Nneworder::neworder &neworder, Norderline::orderline &orderline) {
   int32_t d_id=urand(1,1);
   int32_t c_id=nurand(1023,1,3000);
   int32_t ol_cnt=urand(5,15);

   int32_t supware[15];
   int32_t itemid[15];
   int32_t qty[15];
   for (int32_t i=0; i<ol_cnt; i++) {
      if (urand(1,100)>1)
         supware[i]=w_id; else
         supware[i]=urandexcept(1,warehouses,w_id);
      itemid[i]=nurand(8191,1,100000);
      qty[i]=urand(1,10);
   }

   newOrder(w_id,d_id,c_id,ol_cnt,supware,itemid,qty,now, warehouse, customer, district, stock, item, order, neworder, orderline);
}

void delivery(int32_t w_id, int32_t o_carrier_id, Timestamp datetime, Norder::order &order, Nneworder::neworder &neworder, Norderline::orderline &orderline, Ncustomer::customer &customer){
	for(int d_id =1; d_id <=10; d_id++){
		int o_id = 0;
		Nneworder::neworder::Row *neworderrow;
		for(int i = 1; i <=5; i++){
			neworderrow = neworder.find(std::make_tuple(Integer(w_id), Integer(d_id), Integer(i)));
			if(neworderrow != nullptr){
				o_id = i;
				break;
			}
		}
		if(o_id == 0)
			continue;
		neworder.remove(*neworderrow);

		Norder::order::Row *orderrow = order.find(std::make_tuple(Integer(w_id),Integer(d_id),Integer(o_id)));
		Numeric<2 ,0> o_ol_cnt = orderrow->o_ol_cnt;
		Integer o_c_id = orderrow->o_c_id;
		orderrow->o_carrier_id = o_carrier_id;

		Numeric<6,2> ol_total=0;
		for(int ol_number =1; ol_number < o_ol_cnt.value; ol_number++){
			Norderline::orderline::Row *orderlinerow = orderline.find(std::make_tuple(Integer(w_id),Integer(d_id),Integer(o_id),Integer(ol_number)));
			Numeric<6,2> ol_amount = orderlinerow->ol_amount;
			ol_total += ol_amount;
			orderlinerow->ol_delivery_d = datetime;
		}
		Ncustomer::customer::Row *customerrow = customer.find(std::make_tuple(Integer(w_id), Integer(d_id), Integer(o_c_id)));
		customerrow->c_balance = Numeric<12,2>(customerrow->c_balance.value + ol_total.value);
	}
}

int main(){

	Nwarehouse::warehouse warehouse;
	Ncustomer::customer customer;
	Ndistrict::district district;
	Nstock::stock stock;
	Nitem::item item;
	Norder::order order;
	Nneworder::neworder neworder;
	Norderline::orderline orderline;

	warehouse.init();
	customer.init();
	district.init();
	stock.init();
	item.init();
	order.init();
	neworder.init();
	orderline.init();

	std::clock_t start;
	double duration;

	start = std::clock();

	for(int i = 0; i < 100000; i++){
		int rnd=urand(1,100);
		if (rnd<=10) {
			delivery(urand(1,warehouses), urand(1,10), Timestamp(1000), order, neworder, orderline, customer);
		} else {
			newOrderRandom(Timestamp(1000), urand(1,warehouses), warehouse, customer, district, stock, item, order, neworder, orderline);
		}
	}

	duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
	std::cout << duration << std::endl;

	std::cout << order.orders.size() << std::endl;
	std::cout << neworder.neworders.size() << std::endl;
	std::cout << orderline.orderlines.size() << std::endl;

	return 0;
}
