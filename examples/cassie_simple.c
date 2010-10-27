#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>
#include <string.h>

static char * host = "127.0.0.1";
static int port = 9160;
static int timeout = 1250;

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

void getcolumns_byrange(cassie_t cassie) {

  cassie_column_t columns = NULL, col = NULL;

  cassie_insert_column(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("aaa1"),
      CASSIE_CTOB("vaaa1"),
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

#define CIN(name, value)              \
  cassie_insert_column(               \
      cassie,                         \
      "Keyspace1",                    \
      "Standard1",                    \
      "bob",                          \
      NULL,                           \
      CASSIE_CTOB(name),              \
      CASSIE_CTOB(value),             \
      CASSIE_CONSISTENCY_LEVEL_ONE    \
      );

  CIN("aaa1", "vaaa1")
  CIN("bbb1", "vbbb1")
  CIN("bbb2", "vbbb2")
  CIN("bbb3", "vbbb3")
  CIN("ccc1", "vccc1")
#undef CIN

  columns = cassie_get_columns_by_range(
      cassie,
      "Keyspace1",
      "Standard1",
      "bob",
      NULL,
      CASSIE_CTOB("bbb1"),
      CASSIE_CTOB("bbb3"),
      0,
      0,
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  for (col = columns; col != NULL; col = cassie_column_get_next(col)) {
    printf("\tGot column %s = %s\n", cassie_column_get_name_data(col), cassie_column_get_value_data(col));
  }

  cassie_column_free(columns);

}

void getsupercolumns_bykeys(cassie_t cassie) {

  cassie_blob_t names[3];
  cassie_super_column_t supercols = NULL, supercol = NULL;
  cassie_column_t col = NULL;
  int i = 0;

#define SCIN(name1, name2, value)     \
  cassie_insert_column(               \
      cassie,                         \
      "Keyspace1",                    \
      "Super1",                       \
      "bob",                          \
      CASSIE_CTOB(name1),             \
      CASSIE_CTOB(name2),             \
      CASSIE_CTOB(value),             \
      CASSIE_CONSISTENCY_LEVEL_ONE    \
      );

  SCIN("friend1", "name", "tim")
  SCIN("friend1", "age", "20")
  SCIN("friend2", "name", "mac")
  SCIN("friend2", "age", "30")
  SCIN("friend3", "name", "bud")
  SCIN("friend3", "age", "40")

#undef SCIN

  names[0] = CASSIE_CTOB("friend1");
  names[1] = CASSIE_CTOB("friend2");
  names[2] = NULL;

  supercols = cassie_get_super_columns_by_names(
      cassie,
      "Keyspace1",
      "Super1",
      "bob",
      names,
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  for (supercol = supercols; supercol != NULL; supercol = cassie_super_column_get_next(supercol)) {
    printf("\tGot super column %s\n", cassie_super_column_get_name_data(supercol));
    for (i = 0; i < cassie_super_column_get_num_columns(supercol); i++) {
      col = cassie_super_column_get_column(supercol, i);
      printf("\t\tChild column %s = %s\n", cassie_column_get_name_data(col), cassie_column_get_value_data(col));
    }
  }

}


void getsupercolumns_byrange(cassie_t cassie) {

  cassie_super_column_t supercols = NULL, supercol = NULL;
  cassie_column_t col = NULL;
  int i = 0;

#define SCIN(name1, name2, value)     \
  cassie_insert_column(               \
      cassie,                         \
      "Keyspace1",                    \
      "Super1",                       \
      "bob",                          \
      CASSIE_CTOB(name1),             \
      CASSIE_CTOB(name2),             \
      CASSIE_CTOB(value),             \
      CASSIE_CONSISTENCY_LEVEL_ONE    \
      );

  SCIN("friend1", "name", "tim")
  SCIN("friend1", "age", "20")
  SCIN("friend2", "name", "mac")
  SCIN("friend2", "age", "30")
  SCIN("friend3", "name", "bud")
  SCIN("friend3", "age", "40")

#undef SCIN

  supercols = cassie_get_super_columns_by_range(
      cassie,
      "Keyspace1",
      "Super1",
      "bob",
      CASSIE_CTOB("friend2"),
      CASSIE_CTOB("friend3"),
      0,
      0,
      CASSIE_CONSISTENCY_LEVEL_ONE
      );

  for (supercol = supercols; supercol != NULL; supercol = cassie_super_column_get_next(supercol)) {
    printf("\tGot super column %s\n", cassie_super_column_get_name_data(supercol));
    for (i = 0; i < cassie_super_column_get_num_columns(supercol); i++) {
      col = cassie_super_column_get_column(supercol, i);
      printf("\t\tChild column %s = %s\n", cassie_column_get_name_data(col), cassie_column_get_value_data(col));
    }
  }

}

int main(int argc, char ** argv) {

  cassie_t cassie = cassie_init_with_timeout(host, port, timeout);
  if (!cassie) {
    printf("NO CASSIE\n");
    return(1);
  }

  // Not really needed, but for illustration:
  cassie_set_recv_timeout(cassie, timeout);
  cassie_set_send_timeout(cassie, timeout);

  printf("Simple CRUD:\n");
  crud_simple(cassie);
  printf("\n");

  printf("SuperColumn CRUD:\n");
  crud_super(cassie);
  printf("\n");

  printf("GetColumns by keys:\n");
  getcolumns_bykeys(cassie);
  printf("\n");

  printf("GetColumns by range:\n");
  getcolumns_byrange(cassie);
  printf("\n");

  printf("GetSuperColumns by keys:\n");
  getsupercolumns_bykeys(cassie);
  printf("\n");

  printf("GetSuperColumns by range:\n");
  getsupercolumns_byrange(cassie);
  printf("\n");

  cassie_free(cassie);

  return(0);
}
