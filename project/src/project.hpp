#include <tuple>
#include <utility>
#include "./parser/Semantic.hpp"
#include <unordered_map>
#include <string>
#include <vector>
#include <iostream>
#include "Types.hpp"
#include <fstream>
#include <list>
#include <cstdio>
#include <set>
#include <ctime>
#include <signal.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <atomic>
#include <map>
#include <algorithm>

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

struct Table{
	std::string name;
	struct Attribute{
		std::string name;
		Types::Tag type;
		unsigned len;
		unsigned len1;
		unsigned len2;
		bool notNull;
		Attribute () {}
		Attribute(std::string name, Types::Tag type, unsigned len, unsigned len1, unsigned len2) : name(name), type(type), len(len), len1(len1), len2(len2), notNull(true) {}
	};

	std::vector<Attribute> attributes;
	virtual std::vector<Attribute> getAttributes(){
		return attributes;
	}

	virtual int getColumn(Integer *&result, std::string column, int size, int offset) = 0;
};

struct Customer : Table{
	Customer() {name="customer";}
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
	unsigned size(){return c_id.size();}
	std::vector<Integer> c_id;
	std::vector<Integer> c_d_id;
	std::vector<Integer> c_w_id;
	std::vector<Varchar<16>> c_first;
	std::vector<Char<2>> c_middle;
	std::vector<Varchar<16>> c_last;
	std::vector<Varchar<20>> c_street_1;
	std::vector<Varchar<20>> c_street_2;
	std::vector<Varchar<20>> c_city;
	std::vector<Char<2>> c_state;
	std::vector<Char<9>> c_zip;
	std::vector<Char<16>> c_phone;
	std::vector<Timestamp> c_since;
	std::vector<Char<2>> c_credit;
	std::vector<Numeric<12,2>> c_credit_lim;
	std::vector<Numeric<4,4>> c_discount;
	std::vector<Numeric<12,2>> c_balance;
	std::vector<Numeric<12,2>> c_ytd_paymenr;
	std::vector<Numeric<4,0>> c_payment_cnt;
	std::vector<Numeric<4,0>> c_delivery_cnt;
	std::vector<Varchar<500>> c_data;
	
	std::vector<unsigned> primaryKey;
	virtual int getColumn(Integer* &result, std::string column, int size, int offset){
		int remain = c_id.size() - offset + 1;
		if(column == "c_id"){
			result = &c_id[offset];
		}else if(column == "c_d_id"){
			result = &c_d_id[offset];
		}else if(column == "c_w_id"){
			result = &c_w_id[offset];
		}

		if(size < remain)
			return size;
		else
			return remain;
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
		
		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
			
			c_id.push_back(Integer::castString(data[0].c_str(), data[0].length()));
			c_d_id.push_back(Integer::castString(data[1].c_str(), data[1].length()));
			c_w_id.push_back(Integer::castString(data[2].c_str(), data[2].length()));
			c_first.push_back(Varchar<16>::castString(data[3].c_str(), data[3].length()));
			c_middle.push_back(Char<2>::castString(data[4].c_str(), data[4].length()));
			c_last.push_back(Varchar<16>::castString(data[5].c_str(), data[5].length()));
			c_street_1.push_back(Varchar<20>::castString(data[6].c_str(), data[6].length()));
			c_street_2.push_back(Varchar<20>::castString(data[7].c_str(), data[7].length()));
			c_city.push_back(Varchar<20>::castString(data[8].c_str(), data[8].length()));
			c_state.push_back(Char<2>::castString(data[9].c_str(), data[9].length()));
			c_zip.push_back(Char<9>::castString(data[10].c_str(), data[10].length()));
			c_phone.push_back(Char<16>::castString(data[11].c_str(), data[11].length()));
//			row.c_since =Timestamp::castString(data[12].c_str(), data[12].length());
			c_credit.push_back(Char<2>::castString(data[13].c_str(), data[13].length()));
			c_credit_lim.push_back(Numeric<12,2>::castString(data[14].c_str(), data[14].length()));
			c_discount.push_back(Numeric<4,4>::castString(data[15].c_str(), data[15].length()));
			c_balance.push_back(Numeric<12,2>::castString(data[16].c_str(), data[16].length()));
			c_ytd_paymenr.push_back(Numeric<12,2>::castString(data[17].c_str(), data[17].length()));
			c_payment_cnt.push_back(Numeric<4,0>::castString(data[18].c_str(), data[18].length()));
			c_delivery_cnt.push_back(Numeric<4,0>::castString(data[19].c_str(), data[19].length()));
			c_data.push_back(Varchar<500>::castString(data[20].c_str(), data[20].length()));
		}
	}

};

struct Order : Table{
	Order() {name = "order";}
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

	Row operator [](const int& i) const {return orders[i];}
	unsigned size(){return orders.size();}

	std::vector<Row> orders;
	std::vector<unsigned> primaryKey;

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

		}
	}
	
};

struct Orderline : Table{
	Orderline() {name="orderline";}
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
	Row operator [](const int& i) const {return orderlines[i];}
	unsigned size(){return orderlines.size();}

	std::vector<Row> orderlines;
	std::vector<unsigned> primaryKey;

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

		}
	}
};


struct District : Table{
	District() {name="district";}
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

	std::vector<Row> districts;
	std::vector<unsigned> primaryKey;

	unsigned size(){return districts.size();}

	Row operator [](const int& i) const {return districts[i];}

	std::vector<Row> getAll(){
		return districts;
	}

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

		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
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
};


struct Warehouse : Table{
	Warehouse() {name="warehouse";}
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

	std::vector<Row> warehouses;
	std::vector<unsigned> primaryKey;
	Row operator [](const int& i) const {return warehouses[i];}
	unsigned size(){return warehouses.size();}

	std::vector<Row> getAll(){
		return warehouses;
	}

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

		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
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

		}
	}
};


struct Item : Table{
	Item() {name="item";}

	struct Row{
		Integer i_id;
		Integer i_im_id;
		Varchar<24> i_name;
		Numeric<5,2> i_price;
		Varchar<50> i_data;
	};

	std::vector<Row> items;

	std::vector<unsigned> primaryKey;
	Row operator [](const int& i) const {return items[i];}
	unsigned size(){return items.size();}

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

		while(!f.eof() && f >> line){
			std::vector<std::string> data = split(line, '|');
			Row row;
			row.i_id =Integer::castString(data[0].c_str(), data[0].length());
			row.i_im_id =Integer::castString(data[1].c_str(), data[1].length());
			row.i_name =Varchar<24>::castString(data[2].c_str(), data[2].length());
			row.i_price =Numeric<5,2>::castString(data[3].c_str(), data[3].length());
			row.i_data =Varchar<50>::castString(data[4].c_str(), data[4].length());
			items.push_back(row);


		}
	}

};
