#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>
#include <string.h>

static char * host = "127.0.0.1";
static int port = 9160;

void crud_simple(cassie_t cassie) {

  char *v;

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("age"),
      CASSIE_CTOB("20"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  v = cassie_get_column_value(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("age"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );
  printf("\tValue in column retrieved is: %s\n", v);

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("age"),
      CASSIE_CTOB("35"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  v = cassie_get_column_value(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("age"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );
  printf("\tValue in column retrieved is: %s\n", v);

}

void crud_super(cassie_t cassie) {

  char *v;

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Super1",
      "bob",
      CASSIE_CTOB("attrs"),
      CASSIE_CTOB("age"),
      CASSIE_CTOB("20"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  v = cassie_get_column_value(
      cassie,
      "Keyspace1",
      "Super1",
      "bob",
      CASSIE_CTOB("attrs"),
      CASSIE_CTOB("age"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );
  printf("\tValue in column retrieved is: %s\n", v);

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Super1",
      "bob",
      CASSIE_CTOB("attrs"),
      CASSIE_CTOB("age"),
      CASSIE_CTOB("35"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  v = cassie_get_column_value(
      cassie,
      "Keyspace1",
      "Super1",
      "bob",
      CASSIE_CTOB("attrs"),
      CASSIE_CTOB("age"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );
  printf("\tValue in column retrieved is: %s\n", v);

}

void getcolumns_bykeys(cassie_t cassie) {

  cassie_blob_t names[3];
  cassie_column_t columns = NULL, col = NULL;

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("age"),
      CASSIE_CTOB("20"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("eyes"),
      CASSIE_CTOB("blue"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  names[0] = CASSIE_CTOB("age");
  names[1] = CASSIE_CTOB("eyes");
  names[2] = NULL;

  columns = cassie_get_columns_by_names(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      names,
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  for (col = columns; col != NULL; col = cassie_column_get_next(col)) {
    printf("\tGot column %s = %s\n", cassie_column_get_name_data(col), cassie_column_get_value_data(col));
  }

  cassie_column_free(columns);

}

/*
 *
void getsupercolumns_bykeys(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Super1", "friend1", "name", "tim");
  key_space->insertColumn("bob", "Super1", "friend1", "age", "20");

  key_space->insertColumn("bob", "Super1", "friend2", "name", "mac");
  key_space->insertColumn("bob", "Super1", "friend2", "age", "30");

  key_space->insertColumn("bob", "Super1", "friend3", "name", "bud");
  key_space->insertColumn("bob", "Super1", "friend3", "age", "40");

  vector<string> keys;
  keys.push_back("friend1");
  keys.push_back("friend2");
  vector<org::apache::cassandra::SuperColumn> scols= key_space->getSuperColumns("bob", "Super1", keys);

  for (vector<org::apache::cassandra::SuperColumn>::iterator it = scols.begin(); it != scols.end(); ++it) {
    cout << "\tGot super column " << it->name << endl;
    for (vector<org::apache::cassandra::Column>::iterator it2 = it->columns.begin(); it2 != it->columns.end(); ++it2) {
      cout << "\t\tChild column " << it2->name << " = " << it2->value << endl;
    }
  }

}

void getsupercolumns_byrange(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Super1", "friend1", "name", "tim");
  key_space->insertColumn("bob", "Super1", "friend1", "age", "20");

  key_space->insertColumn("bob", "Super1", "friend2", "name", "mac");
  key_space->insertColumn("bob", "Super1", "friend2", "age", "30");

  key_space->insertColumn("bob", "Super1", "friend3", "name", "bud");
  key_space->insertColumn("bob", "Super1", "friend3", "age", "40");

  org::apache::cassandra::SliceRange range;
  range.start.assign("friend2");
  range.finish.assign("friend3");
  vector<org::apache::cassandra::SuperColumn> scols= key_space->getSuperColumns("bob", "Super1", range);

  for (vector<org::apache::cassandra::SuperColumn>::iterator it = scols.begin(); it != scols.end(); ++it) {
    cout << "\tGot super column " << it->name << endl;
    for (vector<org::apache::cassandra::Column>::iterator it2 = it->columns.begin(); it2 != it->columns.end(); ++it2) {
      cout << "\t\tChild column " << it2->name << " = " << it2->value << endl;
    }
  }

}

void getcolumns_byrange(tr1::shared_ptr<Cassandra> client) {

  Keyspace *key_space= client->getKeyspace("Keyspace1");

  key_space->insertColumn("bob", "Standard1", "aaa1", "vaaa1");
  key_space->insertColumn("bob", "Standard1", "bbb1", "vbbb1");
  key_space->insertColumn("bob", "Standard1", "bbb2", "vbbb2");
  key_space->insertColumn("bob", "Standard1", "bbb3", "vbbb3");
  key_space->insertColumn("bob", "Standard1", "ccc1", "vccc1");

  org::apache::cassandra::SliceRange range;
  range.start.assign("bbb1");
  range.finish.assign("bbb3");
  vector<org::apache::cassandra::Column> cols= key_space->getColumns("bob", "Standard1", range);

  for (vector<org::apache::cassandra::Column>::iterator it = cols.begin(); it != cols.end(); ++it) {
    cout << "\tGot column " << it->name << " = " << it->value << endl;
  }

}

*/

int main(int argc, char ** argv) {

  cassie_t cassie = cassie_init(host, port);

  printf("Simple CRUD:\n");
  crud_simple(cassie);
  printf("\n");

  printf("SuperColumn CRUD:\n");
  crud_super(cassie);
  printf("\n");

  printf("GetColumns by keys:\n");
  getcolumns_bykeys(cassie);
  printf("\n");

/*
  printf("GetColumns by range:\n");
  getcolumns_byrange(cassie);
  printf("\n");

  printf("GetSuperColumns by keys:\n");
  getsupercolumns_bykeys(cassie);
  printf("\n");

  printf("GetSuperColumns by range:\n");
  getsupercolumns_byrange(cassie);
  printf("\n");

  */

  cassie_free(cassie);

  return(0);
}
