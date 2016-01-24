#include <iostream>
#include "Types.hpp"

int main(){
	
	Numeric<2,9> m(50123456789);
	
	void *n = &m;

	Integer nn = 3;
	int ll = 3;

	bool flag = (m == 50123456789);
	std::cout << flag <<std::endl;
return 0;
}
