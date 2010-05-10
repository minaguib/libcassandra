#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>
#include <cassie_column.h>
#include <cassie_blob.h>
#include <cassie_super_column.h>
#include <cassie_io_column.h>
#include <cassie_io_super_column.h>

/*
 * Demonstrates talking to cassandra using the C libcassie
 */

int main(int argc, char ** argv) {

	int i;
	cassie_t cassie;
	cassie_super_column_t supercol;
	cassie_blob_t friends = cassie_blob_init("friends", 7);

	cassie = cassie_init("localhost", 9160);
	cassie_print_debug(cassie);

	/* Add columns in a supercolumn */
	if (!cassie_insert_column_value(cassie, "Keyspace1", "Super2", "joe", "friends", "Bob", "Happy", CASSIE_CONSISTENCY_LEVEL_ONE)) {
		printf("Failed to insert: %s\n", cassie_last_error(cassie));
		exit(1);
	}
	if (!cassie_insert_column_value(cassie, "Keyspace1", "Super2", "joe", "friends", "Albert", "Happy", CASSIE_CONSISTENCY_LEVEL_ONE)) {
		printf("Failed to insert: %s\n", cassie_last_error(cassie));
		exit(1);
	}
	if (!cassie_insert_column_value(cassie, "Keyspace1", "Super2", "joe", "friends", "Vincent", "Happy", CASSIE_CONSISTENCY_LEVEL_ONE)) {
		printf("Failed to insert: %s\n", cassie_last_error(cassie));
		exit(1);
	}

	while (++i) {

		supercol = cassie_get_super_column(cassie, "Keyspace1", "Super2", "joe", friends, CASSIE_CONSISTENCY_LEVEL_ONE);
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
