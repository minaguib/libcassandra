#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>

/*
 * Demonstrates talking to cassandra using the C libcassie
 *
 * This particular example shows supercolumn support
 */

int main(int argc, char ** argv) {

	int i;
	cassie_t cassie;
	cassie_super_column_t supercol;

	cassie = cassie_init("localhost", 9160);
	if (!cassie) {
		printf("NO CASSIE!\n");
		exit(1);
	}
	cassie_print_debug(cassie);

	/* Add columns in a supercolumn */
#define ADDFRIEND(name) \
	if (!cassie_insert_column( \
				cassie, \
				"Keyspace1", \
				"Super2", \
				"joe", \
				CASSIE_CTOB("friends"), \
				CASSIE_CTOB(name), \
				CASSIE_CTOB("Happy"), \
				CASSIE_CONSISTENCY_LEVEL_ONE \
				)) { \
		printf("Failed to insert: %s\n", cassie_last_error(cassie)); \
		exit(1); \
	}

	ADDFRIEND("Albert");
	ADDFRIEND("Bob");
	ADDFRIEND("Vincent");

	while (++i) {

		supercol = cassie_get_super_column(cassie, "Keyspace1", "Super2", "joe", CASSIE_CTOB("friends"), CASSIE_CONSISTENCY_LEVEL_ONE);
		if (!supercol) {
			printf("Failed to get super column: %s\n", cassie_last_error(cassie));
			exit(1);
		}

		// Validate
		if (
				cassie_super_column_get_num_columns(supercol) == 3 &&

				strcmp(cassie_column_get_name_data(cassie_super_column_get_column(supercol, 0)), "Albert") == 0 &&
				strcmp(cassie_column_get_name_data(cassie_super_column_get_column(supercol, 1)), "Bob") == 0 &&
				strcmp(cassie_column_get_name_data(cassie_super_column_get_column(supercol, 2)), "Vincent") == 0
				) {
			printf(".");
		}
		else {
			printf("\nBAD SUPER COLUMN\n");
			exit(1);
		}

		cassie_super_column_free(supercol);

	}

	return(0);
}
