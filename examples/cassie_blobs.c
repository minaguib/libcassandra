#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>
#include <cassie_column.h>

/*
 * Demonstrates talking to cassandra using the C libcassie
 *
 * This particular example manually uses cassie_column_t for inputs and outputs
 * to support the "binary" nature of column names and values in cassandra
 *
 * See cassie_cstrings.c for a simpler, C-compliant-strings interface
 */

int main(int argc, char ** argv) {

	cassie_t cassie;
	int i = 0, j = 0, k = 0;
	cassie_column_t col_in;
	cassie_column_t col_out;

	while (++i) {

		cassie = cassie_init("localhost", 9160);
		cassie_print_debug(cassie);

		printf("Cassie generation %d: ", i);
		for (j = 0; j < 10000; j++) {

			// Prep write blob
			col_in = cassie_column_init(
					cassie,
					"age\r\n", 5,
					"40\r\n", 4,
					0);

			k = cassie_insert_column(
					cassie,
					"Keyspace1",
					"Standard2",
					"joe",
					col_in,
					CASSIE_CONSISTENCY_LEVEL_ONE
					);
			cassie_column_free(col_in);
			if (!k) {
				printf("ERROR: %s\n", cassie_last_error(cassie));
				exit(0);
			}

			col_out = cassie_get_column(
					cassie,
					"Keyspace1",
					"Standard2",
					"joe",
					"age\r\n", 5,
					CASSIE_CONSISTENCY_LEVEL_ONE
				);

			// Validate
			if (
					col_out &&
					col_out->name_len == 5 &&
					memcmp(col_out->name, "age\r\n", 5) == 0 &&
					col_out->value_len == 4 &&
					memcmp(col_out->value, "40\r\n", 4) == 0
					) {
				printf(".");
			}
			else {
				printf("BAD OUTPUT COLUMP\n");
				exit(1);
			}

			cassie_column_free(col_out);
		}
		printf("\n");

		cassie_free(cassie);
		cassie = NULL;
	}

	return(0);
}
