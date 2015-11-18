
#include<string>

struct Query{
std::vector<std::string> relations;
std::vector<std::string> selectnames;
std::vector<std::pair<std::string, std::string>> joinconditions;
std::vector<std::pair<std::string, std::string>> selectconditions;
};
