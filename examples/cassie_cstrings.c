#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>
#include <cassie_io_column.h>

/*
 * Demonstrates talking to cassandra using the C libcassie
 *
 * This particular example uses the simpler interface which does not
 * support the "binary" nature of column names and values in cassandra
 *
 * See cassie_blobs.c for the richer interface
 *
 */

int main(int argc, char ** argv) {

	cassie_t cassie;
	int i = 0, j = 0, k = 0;
	char * v;

	while (++i) {

		cassie = cassie_init("localhost", 9160);
		cassie_print_debug(cassie);

		printf("Cassie generation %d: ", i);
		for (j = 0; j < 10000; j++) {

			// Write via friendly C-strings
			k = cassie_insert_column_value(
					cassie,
					"Keyspace1",
					"Standard2",
					"joe",
					NULL,
					"age",
					"20",
					CASSIE_CONSISTENCY_LEVEL_ONE
					);
			if (!k) {
				printf("ERROR: %s\n", cassie_last_error(cassie));
				exit(0);
			}

			// Read via friendly C-strings
			v = cassie_get_column_value(
					cassie,
					"Keyspace1",
					"Standard2",
					"joe",
					NULL,
					"age",
					CASSIE_CONSISTENCY_LEVEL_ONE
				);
			// Validate
			if (v && strcmp(v, "20") == 0) {
				printf(".");
			}
			else {
				printf("BAD VALUE\n");
				exit(1);
			}
			free(v);
		}
		printf("\n");

		cassie_free(cassie);
		cassie = NULL;
	}

	return(0);
}
