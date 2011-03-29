#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>

/*
 * Demonstrates talking to cassandra using the C libcassie
 *
 * This particular example demonstrates various aspects of working with blobs:
 * * Using the cassie_blob_init/cassie_blob_free
 * * Using CASSIE_CTOB, CASSIE_BDATA, CASSIE_BLENGTH
 *
 */

int main(int argc, char ** argv) {

	cassie_t cassie;
	int i = 0, j = 0, k = 0;
	cassie_blob_t image;
	cassie_column_t col_out;
	char gif[] = {
		0x47,0x49,0x46,0x38,0x37,0x61,0x01,0x00,0x01,0x00,0x80,0x00,0x00,0xff,0xff,0xff,
		0xff,0xff,0xff,0x2c,0x00,0x00,0x00,0x00,0x01,0x00,0x01,0x00,0x00,0x02,0x02,0x44,
		0x01,0x00,0x3b
	};

	image = cassie_blob_init(gif, 35);

	while (++i) {

		cassie = cassie_init("localhost", 9160);
		if (!cassie) {
			printf("NO CASSIE!\n");
			exit(1);
		}
		cassie_print_debug(cassie);

		if (!cassie_set_keyspace(cassie, "Keyspace1")) {
			printf("Error setting keyspace: %s\n", cassie_last_error_string(cassie));
			return(1);
		}

		printf("Cassie generation %d: ", i);
		for (j = 0; j < 10000; j++) {

			k = cassie_insert_column(
					cassie,
					"Standard2",
					CASSIE_CTOB("joe"),
					NULL,
					CASSIE_CTOB("pic"),
					image,
					CASSIE_CONSISTENCY_LEVEL_ONE
					);

			if (!k) {
				printf("ERROR: %s\n", cassie_last_error_string(cassie));
				exit(0);
			}

			col_out = cassie_get_column(
					cassie,
					"Standard2",
					CASSIE_CTOB("joe"),
					NULL,
					CASSIE_CTOB("pic"),
					CASSIE_CONSISTENCY_LEVEL_ONE
				);

			// Validate
			if (

					col_out &&

					CASSIE_BLENGTH(cassie_column_get_name(col_out)) == 3 &&
					strcmp(CASSIE_BDATA(cassie_column_get_name(col_out)), "pic") == 0 &&

					CASSIE_BLENGTH(cassie_column_get_value(col_out)) == CASSIE_BLENGTH(image) &&
					memcmp(CASSIE_BDATA(cassie_column_get_value(col_out)), CASSIE_BDATA(image), CASSIE_BLENGTH(image)) == 0

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

	cassie_blob_free(image);

	return(0);
}
