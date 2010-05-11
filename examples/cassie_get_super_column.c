#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>

/*
 * Demonstrates talking to cassandra using the C libcassie
 */

int main(int argc, char ** argv) {

	int i;
	cassie_t cassie;
	cassie_super_column_t supercol;

	cassie = cassie_init("localhost", 9160);
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

	ADDFRIEND("Bob");
	ADDFRIEND("Albert");
	ADDFRIEND("Vincent");

	while (++i) {

		supercol = cassie_get_super_column(cassie, "Keyspace1", "Super2", "joe", CASSIE_CTOB("friends"), CASSIE_CONSISTENCY_LEVEL_ONE);
		if (!supercol) {
			printf("Failed to get super column: %s\n", cassie_last_error(cassie));
			exit(1);
		}

		// Validate
		if (supercol->num_columns == 3) {
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
