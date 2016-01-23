#include <iostream>
#include "Types.hpp"

int main(){
	
	Numeric<2,9> m(50123456789);
	
	void *n = &m;
	bool flag = (m == *(Numeric<2,9>*)(n));
	std::cout << flag <<std::endl;
	std::cout <<*(Numeric<4,6>*)(n)<< std::endl;
	return 0;
}
