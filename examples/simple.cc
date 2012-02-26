#include <string.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <set>
#include <string>
#include <stdio.h>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/column_family_definition.h>
#include <libcassandra/keyspace.h>
#include <libcassandra/keyspace_definition.h>

using namespace std;
using namespace libcassandra;

static string host("127.0.0.1");
static int port= 9160;
static int timeout= 5000;

int main()
{

  CassandraFactory factory(host, port);
  boost::shared_ptr<Cassandra> client(factory.create());

  // Not really needed since the factory timeout sets all 3 by default:
  client->setRecvTimeout(timeout);
  client->setSendTimeout(timeout);

  string clus_name= client->getClusterName();
  cout << "cluster name: " << clus_name << endl;

  vector<KeyspaceDefinition> key_out= client->getKeyspaces();
  for (vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it)
  {
    cout << "keyspace: " << (*it).getName() << endl;
  }

  try
  {
    /* create keyspace */
    KeyspaceDefinition ks_def;
    ks_def.setName("drizzle");
    client->createKeyspace(ks_def);
    client->setKeyspace(ks_def.getName());

    /* create standard column family */
    ColumnFamilyDefinition cf_def;
    cf_def.setName("Data");
    cf_def.setKeyspaceName(ks_def.getName());
    client->createColumnFamily(cf_def);

    /* insert data */
    client->insertColumn("sarah", "Data", "third", "this is data being inserted!");
    /* retrieve that data */
    string res= client->getColumnValue("sarah", "Data", "third");
    cout << "Value in column retrieved is: " << res << endl;
  }
  catch (org::apache::cassandra::InvalidRequestException &ire)
  {
    cout << ire.why << endl;
    return 1;
  }

  return 0;
}
