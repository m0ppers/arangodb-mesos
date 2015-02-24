#include <iostream>
#include <mesos/resources.hpp>

using namespace mesos;
using namespace std;

int main () {
  Resources a = Resources::parse("cpus(*):3; cpus(arangodb):2").get();

  cout << a << endl;

  return 0;
}
