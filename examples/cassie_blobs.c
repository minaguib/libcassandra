#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>

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
	cassie_blob_t age, fourty;
	cassie_column_t col_out;

	age = cassie_blob_init("age\r\n", 5);
	fourty = cassie_blob_init("40\r\n", 4);

	while (++i) {

		cassie = cassie_init("localhost", 9160);
		cassie_print_debug(cassie);

		printf("Cassie generation %d: ", i);
		for (j = 0; j < 10000; j++) {

			k = cassie_insert_column(
					cassie,
					"Keyspace1",
					"Standard2",
					"joe",
					NULL,
					age,
					fourty,
					CASSIE_CONSISTENCY_LEVEL_ONE
					);

			if (!k) {
				printf("ERROR: %s\n", cassie_last_error(cassie));
				exit(0);
			}

			col_out = cassie_get_column(
					cassie,
					"Keyspace1",
					"Standard2",
					"joe",
					NULL,
					age,
					CASSIE_CONSISTENCY_LEVEL_ONE
				);

			// Validate
			if (
					col_out &&
					col_out->name->length == age->length &&
					memcmp(col_out->name->data, age->data, age->length) == 0 &&
					col_out->value->length == fourty->length &&
					memcmp(col_out->value->data, fourty->data, fourty->length) == 0
					) {
				printf(".");
			}
			else {
				printf("BAD OUTPUT COLUMN\n");
				exit(1);
			}

			cassie_column_free(col_out);
		}
		printf("\n");

		cassie_free(cassie);
		cassie = NULL;
	}

	cassie_blob_free(fourty);
	cassie_blob_free(age);

	return(0);
}
