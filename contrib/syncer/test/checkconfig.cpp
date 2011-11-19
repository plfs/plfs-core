#include <iostream>
#include "Conf.hxx"

using namespace std;

int main(int argc, char **argv)
{
    if (argc < 2)
	return -1;
    Conf conf(argv[1]);
    map<string, string>::iterator itr;

    cout<<"There will be "<<conf.get_threads_num()<<" threads"<<endl;
    for (itr = conf.mapping.begin(); itr != conf.mapping.end(); itr++) {
	cout << itr->first << " -> " << itr->second << endl;
    }
    return 0;
}
