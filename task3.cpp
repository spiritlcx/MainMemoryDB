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
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <atomic>
#include <map>

namespace Types {
   enum class Tag : unsigned {Integer=0, Char, Varchar, Numeric, Timestamp};
}

std::vector<std::string> split(std::string s, char delimit){
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
	typedef std::unordered_map<const key_t,int,key_hash,key_equal> map_t;
	std::vector<Row> customers;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;
	std::vector<Row> getAll(){
		return customers;
	}
	Row get(int index){
		return customers[index];
	}

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
		int count = 0;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
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

			m[std::make_tuple(row.c_w_id,row.c_d_id,row.c_id)] = count++;
		}
	}
	int find(std::tuple<Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return itr->second;
		}
		else{
			return -1;
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
	typedef std::unordered_map<const key_t,int,key_hash,key_equal> map_t;
	std::vector<Row> orders;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;

	std::vector<Row> getAll(){
		return orders;
	}

	Row get(int index){
		return orders[index];
	}
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
		int count = 0;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
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

			m[std::make_tuple(row.o_w_id,row.o_d_id,row.o_id)] = count++;
		}
	}
	int find(std::tuple<Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return itr->second;
		}
		else{
			return -1;
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
	typedef std::unordered_map<const key_t,int,key_hash,key_equal> map_t;
	std::vector<Row> orderlines;
	std::vector<Attribute> attributes;
	std::vector<unsigned> primaryKey;
	map_t m;

	std::vector<Row> getAll(){
		return orderlines;
	}

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
		int count = 0;
		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
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

			m[std::make_tuple(row.ol_w_id,row.ol_d_id,row.ol_o_id,row.ol_number)] = count++;
		}
	}
	int find(std::tuple<Integer,Integer,Integer,Integer> tuple){
		auto itr = m.find(tuple);
		if(itr!=m.end()){
			return itr->second;
		}
		else{
			return -1;
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

struct orderkey{
	Integer o_d_id;
	Integer o_w_id;
	Integer o_id;
	Numeric<12, 2> c_balance;
	Numeric<2, 0> o_ol_cnt;
};

double query(Norder::order &order, Ncustomer::customer &customer, Norderline::orderline &orderline){
	std::vector<Norder::order::Row> orders = order.getAll();
	std::vector<Ncustomer::customer::Row> customers = customer.getAll();
	std::vector<Norderline::orderline::Row> orderlines = orderline.getAll();

	std::vector<orderkey> orderkeys;

		typedef std::tuple<Integer,Integer,Integer> key_customer;
		struct key_hash_customer : public std::unary_function<key_customer, std::size_t>{
			std::size_t operator()(const key_customer& k) const{
				return std::get<0>(k).value+std::get<1>(k).value+std::get<2>(k).value;
			}
		};
		struct key_equal_customer : public std::binary_function<key_customer, key_customer, bool>{
			bool operator()(const key_customer& v0, const key_customer& v1) const{
				return (
					std::get<0>(v0) == std::get<0>(v1)&&
					std::get<1>(v0) == std::get<1>(v1)&&
					std::get<2>(v0) == std::get<2>(v1)
				);
			}
		};


	typedef std::unordered_map<const key_customer , int,key_hash_customer, key_equal_customer> map_customer;



	map_customer m_customer;
	int count = 0;
	for(Ncustomer::customer::Row c_ustomer : customers){
		if(c_ustomer.c_last.value[0] != 'B')
			continue;
		m_customer[std::make_tuple(c_ustomer.c_w_id, c_ustomer.c_d_id, c_ustomer.c_id)] = count++;
	}
	for(Norder::order::Row o_rder : orders){
		
		auto itr = m_customer.find(std::make_tuple(o_rder.o_w_id, o_rder.o_d_id, o_rder.o_c_id));

		if(itr != m_customer.end()){
			orderkey key;
			key.o_w_id = o_rder.o_w_id;
			key.o_d_id = o_rder.o_d_id;
			key.o_id = o_rder.o_id;
			key.c_balance = customers[itr->second].c_balance;
			key.o_ol_cnt = o_rder.o_ol_cnt;
			orderkeys.push_back(key);
		}
		
	}
	typedef std::tuple<Integer,Integer,Integer> key_orderline;
	
	std::unordered_map<key_orderline, int, key_hash_customer,key_equal_customer> m_orderkey;
	
	count = 0;
	for(orderkey key : orderkeys){
		m_orderkey[std::make_tuple(key.o_id, key.o_d_id, key.o_w_id)]= count++;
	}

	double sum = 0;
	for(Norderline::orderline::Row o_rderline : orderlines){
		auto itr = m_orderkey.find(std::make_tuple(o_rderline.ol_o_id, o_rderline.ol_d_id,o_rderline.ol_w_id));
		if(itr != m_orderkey.end()){
			sum += (o_rderline.ol_quantity.value * o_rderline.ol_amount.value - orderkeys[itr->second].c_balance.value * orderkeys[itr->second].o_ol_cnt.value);
		}
	}
	return sum;

}

template <typename T>
class Input{
public:
	virtual std::vector<T> produce() = 0;
};

template <typename T>
class TableScan : public Input<T>{
public:
	TableScan(const std::vector<T>& table) : table(table){};
	std::vector<T> produce();
private:
	std::vector<T> table;
};

template<typename T> std::vector<T> TableScan<T>::produce(){
	return table;
}

template<typename T>
class Selection : public Input<T>{
public:
	Selection(Input<T>* input, bool(*p)(T tuple)) : input(input),p(p){};
	std::vector<T> produce();
	std::vector<T> consume(std::vector<T>& tuples, bool(*p)(T tuple));
private:
	Input<T>* input;
	bool(*p)(T tuple);
};

template<typename T> bool _select(T tuple){
	if(tuple.c_id == 322 && tuple.c_w_id == 1 &&tuple.c_d_id==1){
		return true;
	}else{
		return false;
	}
}

template<typename T> std::vector<T> Selection<T>::produce(){
	std::vector<T> tuples = input->produce();
	
	std::vector<T> results = consume(tuples, _select);
	return results;
}

template<typename T> std::vector<T> Selection<T>::consume(std::vector<T>& tuples, bool(*p)(T tuple)){
	std::vector<T> result;

	for(T tuple : tuples){
		if(p(tuple)){
			result.push_back(tuple);
		}
	}
	
	return result;
}

template<typename T>
void format(T tuple){
	std::cout <<tuple.c_first <<" "<<tuple.c_last <<" "<<tuple.o_all_local << " "<<tuple.ol_amount <<std::endl;
}


template<typename T>
class Print{
public:
Print(Input<T>* input, void(*format)(T tuple)) : input(input), format(format){};
void produce();
void consume(std::vector<T>& tuples, void(*format)(T tuple));
private:
Input<T>* input;
void(*format)(T tuple);
};

template<typename T> void Print<T>::produce(){
std::vector<T> tuples = input->produce();	
	consume(tuples, format);
	
}

template<typename T> void Print<T>::consume(std::vector<T>& tuples, void(*format)(T tuple)){
	for(T tuple : tuples){
		format(tuple);
	}
}

template<typename T, typename I,typename L, typename R>
class Hashjoin : public Input<T>{
public:
	Hashjoin(Input<L>* left, Input<R>* right, T(*combine)(L l, R r), void(*index)(I&m, L tuple, int count), std::vector<int>(*find)(I& m, R tuple)) : left(left), right(right), combine(combine), index(index), find(find){};

//	Hashjoin(Input<L>*left, Input<R>* right, T(*combine)(L l, R r)) : left(left), right(right), combine(combine){};
	std::vector<T> produce();
	I consumeFromLeft(std::vector<L> lefttable);
	std::vector<T> consumeFromRight(I m, std::vector<L> lefttable, std::vector<R> righttable);
private:
	Input<L>* left;
	Input<R>* right;
	T (*combine)(L l, R r);
	void (*index)(I& m, L tuple, int count);
	std::vector <int>(*find)(I& m, R tuple);
};

template<typename T, typename I, typename L, typename R> std::vector<T> Hashjoin<T, I, L ,R>::produce(){
	std::vector<L> lefttable = left->produce();
	std::vector<R> righttable = right->produce();
		
	I m = consumeFromLeft(lefttable);

	
	return consumeFromRight(m, lefttable, righttable);
}


template<typename T, typename I, typename L, typename R> I Hashjoin<T, I, L, R>::consumeFromLeft(std::vector<L> lefttable){
	I m;
	int count = 0;

	for(L tuple : lefttable){
		index(m, tuple, count++);
	}
	return m;
}


template<typename T, typename I, typename L, typename R> std::vector<T> Hashjoin<T, I, L, R>::consumeFromRight(I m, std::vector<L> lefttable, std::vector<R> righttable){
	std::vector<T> result;
	for(R tuple : righttable){
		std::vector<int> indices = find(m, tuple);
		for(int i : indices){
			T t = combine(lefttable[i], tuple);
			result.push_back(t);
		}
	}
	return result;
}

struct operation{
	std::string name;
	std::string id;
	std::string type;
	std::string table;
	std::vector<std::string> printcondition;
	std::vector<std::pair<std::string, std::string>> selectcondition;
	std::vector<std::pair<std::string, std::string>> joincondition;
	std::vector<std::pair<std::string, std::string>> attribute;
	operation* left;
	operation* right;

};

void compile(operation *head){
	operation* current = head;

	if(current == nullptr){
		return;
	}

	if(current->left != nullptr){
		compile(current->left);
	}

	if(current->left != nullptr){
		compile(current->right);
	}

	
	if(current->name == "select"){
		std::cout << "template<typename T> bool _select"+current->id+"(T tuple){"<<std::endl;
		
		std::cout << "if(";
		int count = 0;
		int size = current->selectcondition.size();
		for(std::pair<std::string,std::string> condition : current->selectcondition){
			std::cout <<"tuple."+condition.first +"=="+condition.second;
			if(++count < size){
				std::cout << " && ";
			}
		}
		std::cout << "){" << std::endl;
		std::cout <<"\treturn true; "<< std::endl;
		std::cout << "}else{" << std::endl;
		std::cout << "\treturn false;" << std::endl;
		std::cout << "}" << std::endl;
		std::cout << "}" << std::endl;
		std::cout << "Selection<"+current->type+"> *select"+current->id+" = new Selection<"+current->type+">("+current->left->name+current->left->id+",_select"+current->id+");" << std::endl;	
		std::cout << std::endl;	
	}else if(current->name == "tableScan"){
		std::cout << "Input<"+current->type+"> *tableScan"+current->id+" = new TableScan<"+current->type+">("+current->table+".getAll());" << std::endl;	
		std::cout << std::endl;
	}else if(current->name == "hashjoin"){
		std::cout << "struct hashjoin"+current->id + "T{" << std::endl;
		for(std::pair<std::string, std::string> attribute: current->attribute){
			std::cout << "\t"+attribute.first + " " + attribute.second << ";" << std::endl;
		}
		
		std::cout << "bool checkmember(std::string member){" << std::endl;
		std::cout << "\tif(";
		int count = 0;
		int size = current->attribute.size();
		for(std::pair<std::string, std::string> attribute : current->attribute){
			std::cout << "member=="+attribute.second;
			if(++count < size){
				std::cout << "||";
			}
		}
		std::cout << ")" <<std::endl;
		std::cout << "\t\treturn true;" << std::endl;
		std::cout << "\telse" << std::endl;
		std::cout << "\t\treturn false;" << std::endl;
		


		std::cout << "}" << std::endl;

		std::cout << "};" << std::endl;
		
		std::cout << "template<typename T, typename L, typename R> T combine"+current->id+"(L l, R r){"<<std::endl;
		std::cout << "\tT t;" << std::endl;
		for(std::pair<std::string, std::string> attribute : current->left->attribute){
			std::cout << "if(l.check(" + attribute.second + "))"<<std::endl;
			std::cout << "\tt." + attribute.second + " = l." + attribute.second + ";" << std::endl;
			std::cout << "else" <<std::endl;
			std::cout << "\tt." + attribute.second + " = r." + attribute.second + ";" << std::endl;
		
		}
		
		std::cout << "\treturn t;" << std::endl;	
		std::cout << "}" << std::endl;


		std::cout << "template<typename I, typename L> void index"+current->id+"(I& m, L tuple, int count){"<<std::endl;
	
		std::cout << "\tm.insert(std::make_pair(std::make_tuple(";

		for(std::pair<std::string,std::string> pair:current->joincondition){
			std::cout <<"tuple." << pair.first;
			if(++count < size){
				std::cout << ", ";
			}
		}
		std::cout << "), count));" << std::endl;
		std::cout << "}" << std::endl;


		std::cout << "template<typename I, typename R> std::vector<int> find"+current->id+"(I& m, R tuple){"<<std::endl;
		std::cout << "\tstd::vector<int> indices;" << std::endl;
		std::cout << "\tauto itr = m.find(std::make_tuple(";
		count = 0;
		for(std::pair<std::string,std::string> pair:current->joincondition){
			std::cout <<"tuple." << pair.second;
			if(++count < size){
				std::cout << ", ";
			}
		}
		std::cout << "));" << std::endl;
		std::cout << "\twhile(itr != m.end()){" << std::endl;
		std::cout << "\t\tindices.push_back(itr->second);" << std::endl;
		std::cout << "\t\titr++;"<<std::endl;
		std::cout << "\t}" << std::endl;
		std::cout << "\treturn indices;" << std::endl;
		std::cout << "}" << std::endl;
		std::cout << std::endl;
		std::cout << "Hashjoin<"+current->name+current->id+"T, std::unordered_multimap<std::tuple<";
		count = 0;
		size = current->joincondition.size();
		while(count < size){
			std::cout << "Integer";
			if(++count < size){
				std::cout << ",";
			}
		}
		std::cout << ">, int>, "+current->left->type+", "+current->right->type+"> *hashjoin" +current->id + "= new Hashjoin<"+current->name+current->id+"T, std::unordered_multimap<std::tuple<";
	count = 0;	
		while(count < size){
			std::cout << "Integer";
			if(++count < size){
				std::cout << ",";
			}
		}	
		
		std::cout<<">,std::vector<int>>, "<< current->left->type+", "+current->right->type+">("+current->left->name + current->left->id <<","<< current->right->name+current->right->id+",combine"+current->id+",index"+current->id+",find"+current->id+");"<< std::endl;
	
	}else if(current->name == "print"){
		std::cout << "template<typename T>" << std::endl;
		std::cout << "void "<<"format"+current->id<<"(T tuple){"<<std::endl;
		std::cout << "\tstd::cout ";
		for(std::string condition : current->printcondition){
			std::cout <<"<<\" \""<< "<<tuple."+condition;
		}
		std::cout <<";" << std::endl;
		std::cout <<"}" << std::endl;
		std::cout << "Print<"+current->type+",format"+current->id+"> *print"+current->id+" = new Print<"+current->type+","+"format"+current->id+">("+current->left->name+current->left->id+")" << std::endl;
	}
}



template<typename T> bool _selectA(T tuple){
if(tuple.c_id==322 && tuple.c_w_id==1 && tuple.c_d_id==1){
	return true; 
}else{
	return false;
}
}
	
struct hashjoinAT{
	Varchar<16> c_first;
	Varchar<16> c_last;
	Integer o_w_id;
	Integer o_d_id;
	Integer o_c_id;
	Integer o_id;
	Numeric<1,0> o_all_local;
	bool checkmember(std::string member){
		if(member != "c_first" && member != "c_last"&& member != "o_w_id"&& member != "o_d_id "&& member != "o_c_id"&& member != "o_id"&& member != "o_all_local")
		return false;
		else{
			return true;
		}
	}
};

template<typename T, typename L, typename R> T combineA(L l, R r){
	T t;
	t.c_first = l.c_first;
	t.c_last = l.c_last;
	t.o_w_id = r.o_w_id;
	t.o_d_id = r.o_d_id;
	t.o_c_id = r.o_c_id;
	t.o_id = r.o_id;
	t.o_all_local = r.o_all_local;

	return t;
}

template<typename I, typename L> void indexA(I& m, L tuple, int count){
	m.insert(std::make_pair(std::make_tuple(tuple.c_w_id, tuple.c_d_id, tuple.c_id), count));
}

template<typename I, typename R> std::vector<int> findA(I& m, R tuple){
	std::vector<int> indices;
	auto itr = m.find(std::make_tuple(tuple.o_w_id, tuple.o_d_id, tuple.o_c_id));
	while(itr != m.end()){
		indices.push_back(itr->second);
		itr++;
	}
	return indices;
}


struct hashjoinBT{		
	Varchar<16> c_first;
	Varchar<16> c_last;
	Numeric<1,0> o_all_local;
	Numeric<6,2> ol_amount;
};

template<typename T, typename L, typename R> T combineB(L l, R r){
	T t;
	t.c_first = l.c_first;
	t.c_last = l.c_last;
	t.o_all_local = l.o_all_local;
	t.ol_amount = r.ol_amount;
	return t;
	}
		
template<typename I, typename L> void indexB(I& m, L tuple, int count){
		m.insert(std::make_pair(std::make_tuple(tuple.o_w_id, tuple.o_d_id, tuple.o_id), count));
}
		
template<typename I, typename R> std::vector<int> findB(I& m, R tuple){
	std::vector<int> indices;
	auto itr = m.find(std::make_tuple(tuple.ol_w_id, tuple.ol_d_id, tuple.ol_o_id));
	while(itr != m.end()){
		indices.push_back(itr->second);
		itr++;
	}
	return indices;
}
	

int main(){

	Ncustomer::customer customer;
	Norder::order order;
	Norderline::orderline orderline;

	customer.init();
	order.init();
	orderline.init();


	operation* l5 = new struct operation();
	l5->name = "tableScan";
	l5->id = "A";
	l5->type = "Ncustomer::customer::Row";
	l5->table = "customer";

	operation* l4 = new struct operation();
	l4->name = "select";
	l4->id = "A";
	l4->type = "Ncustomer::customer::Row";
	l4->table = "customer";
	l4->attribute.push_back(std::make_pair("Varchar<16>","c_first"));
	l4->attribute.push_back(std::make_pair("Varchar<16>","c_last"));	
	l4->selectcondition.push_back(std::make_pair("c_id", "322"));
	l4->selectcondition.push_back(std::make_pair("c_w_id", "1"));
	l4->selectcondition.push_back(std::make_pair("c_d_id", "1"));
	l4->left=l5;

	operation* l6 = new struct operation();
	l6->name = "tableScan";
	l6->id = "B";
	l6->type = "Norder::order::Row";
	l6->table="order";

	operation* l2 = new struct operation();
	l2->name = "hashjoin";
	l2->type = "hashjoinAT";
	l2->id = "A";
	l2->joincondition.push_back(std::make_pair("c_w_id","o_w_id"));
	l2->joincondition.push_back(std::make_pair("c_d_id","o_d_id"));
	l2->joincondition.push_back(std::make_pair("c_id","o_c_id"));
	l2->attribute.push_back(std::make_pair("Integer","o_w_id"));
	l2->attribute.push_back(std::make_pair("Integer","o_d_id"));
	l2->attribute.push_back(std::make_pair("Integer","o_c_id"));
	l2->attribute.push_back(std::make_pair("Varchar<16>","c_first"));
	l2->attribute.push_back(std::make_pair("Varchar<16>","c_last"));

	l2->left = l4;
	l2->right = l6;

	operation* l3 = new struct operation();
	l3->name = "tableScan";
	l3->id = "C";
	l3->type = "Norderline::orderline::Row";
	l3->table = "orderline";	

	operation* l1 = new struct operation();
	l1->name = "hashjoin";
	l1->id = "B";
	l1->joincondition.push_back(std::make_pair("o_w_id","ol_w_id"));
	l1->joincondition.push_back(std::make_pair("o_d_id","ol_d_id"));
	l1->joincondition.push_back(std::make_pair("o_id","ol_o_id"));
	l1->attribute.push_back(std::make_pair("Integer","c_first"));
	l1->attribute.push_back(std::make_pair("Integer","c_last"));
	l1->attribute.push_back(std::make_pair("Integer","o_all_local"));
	l1->attribute.push_back(std::make_pair("Integer","ol_amount"));


	l1->left = l2;
	l1->right = l3;
	

	operation* l0 = new struct operation();
	l0->name = "print";
	l0->id = "A";
	l0->type = "hashjoinBT";
	l0->left = l1;
	l0->printcondition.push_back("c_first");
	l0->printcondition.push_back("c_last");
	l0->printcondition.push_back("o_all_local");
	l0->printcondition.push_back("ol_amount");

//	compile(l1);


	Input<Ncustomer::customer::Row> *tableScanA = new TableScan<Ncustomer::customer::Row>(customer.getAll());
	Selection<Ncustomer::customer::Row> *selectA = new Selection<Ncustomer::customer::Row>(tableScanA, _selectA);

	Input<Norder::order::Row> *tableScanB = new TableScan<Norder::order::Row>(order.getAll());

//	Hashjoin<hashjoinAT, std::multimap<std::tuple<Integer,Integer,Integer>, std::vector<int>>, Ncustomer::customer::Row, Norder::order::Row> *hashjoinA= new Hashjoin<hashjoinAT, std::multimap<std::tuple<Integer,Integer,Integer>,std::vector<int>>, Ncustomer::customer::Row, Norder::order::Row>(selectA,tableScanB, combineA);
	
	Hashjoin<hashjoinAT, std::multimap<std::tuple<Integer,Integer,Integer>, int>, Ncustomer::customer::Row, Norder::order::Row> *hashjoinA= new Hashjoin<hashjoinAT, std::multimap<std::tuple<Integer,Integer,Integer>,int>, Ncustomer::customer::Row, Norder::order::Row>(selectA,tableScanB, combineA,indexA,findA);
					
	Input<Norderline::orderline::Row> *tableScanC = new TableScan<Norderline::orderline::Row>(orderline.getAll());



	Hashjoin<hashjoinBT, std::multimap<std::tuple<Integer,Integer,Integer>, int>, hashjoinAT, Norderline::orderline::Row> *hashjoinB= new Hashjoin<hashjoinBT, std::multimap<std::tuple<Integer,Integer,Integer>,int>, hashjoinAT, Norderline::orderline::Row>(hashjoinA,tableScanC, combineB, indexB, findB);

	Print<hashjoinBT> *printA = new Print<hashjoinBT>(hashjoinB, format);

	printA->produce();



//std::multimap<std::tuple<int,int>, std::vector<int>> m;







//	Input<Ncustomer::customer::Row> *tableScanA = new TableScan<Ncustomer::customer::Row>(customer.getAll());

//	Print<Ncustomer::customer::Row> *printA = new Print<Ncustomer::customer::Row>(tableScanA, format);

//	printA->produce();
	



//	Input<Ncustomer::customer::Row> *tableScan = new TableScan<Ncustomer::customer::Row>(customer.getAll());
//	std::vector<Ncustomer::customer::Row> k = tableScan->produce();

//	Selection<Ncustomer::customer::Row> *selec = new Selection<Ncustomer::customer::Row>(tableScan);
	

//	Print<Ncustomer::customer::Row> *print = new Print<Ncustomer::customer::Row>(selec);
//	print->produce();

//	Input<Ncustomer::customer::Row> *leftScan = new TableScan<Ncustomer::customer::Row>(customer.getAll());
	

//	Input<Norder::order::Row> *rightScan = new TableScan<Norder::order::Row>(order.getAll());

//	Hashjoin<Ncustomer::customer::Row, Ncustomer::customer::map_t, Ncustomer::customer::Row, Norder::order::Row> *hashjoin = new Hashjoin<Ncustomer::customer::Row, Ncustomer::customer::map_t, Ncustomer::customer::Row, Norder::order::Row>(leftScan, rightScan); 

//	hashjoin->produce();


	
//	std::clock_t start;
//	double duration;

//	start = std::clock();

//	double sum = 0;

//	int querynumber = 0;
//	for(unsigned i = 0; i < 10; i++){
//		querynumber++;
//		start=std::clock();
//		sum = query(order,customer,orderline);
//		duration = ( std::clock() - start ) / (double) CLOCKS_PER_SEC;
//		std::cout << "duration is " << duration << std::endl;
//		std::cout << "sum is " << sum << std::endl;		
//	}
	
	

	return 0;
}
