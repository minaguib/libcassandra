#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cassie.h>
#include <string.h>

static char * host = "127.0.0.1";
static int port = 9160;
static int timeout = 1250;

#define RAND_KEY_LEN 36
#define RAND_KEY_CHARS "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

char * rand_key(void) {
	static int chars_len = strlen(RAND_KEY_CHARS);
	char *key = malloc(RAND_KEY_LEN + 1);
	char *temp;
	int i;

	for (i = 0, temp = key; i < RAND_KEY_LEN; i++, temp = key + i) {
		*temp = RAND_KEY_CHARS[random() % chars_len];
	}
	*temp = '\0';

	return key;
}

int cassie_do_one(void) {

	int retval = 0;
	char * key = NULL;
	cassie_t cassie  = NULL;
	cassie_column_t col = NULL;

	if ((cassie = cassie_init_with_timeout(host, port, timeout))) {
		if (cassie_set_keyspace(cassie, "Keyspace1")) {
			key = rand_key();
			if (cassie_insert_column(
					cassie,
					"Standard1",
					CASSIE_CTOB(key),
					NULL,
					CASSIE_CTOB("age"),
					CASSIE_CTOB("20"),
					CASSIE_CONSISTENCY_LEVEL_ONE
					)) {
				if ((col = cassie_get_column(
					cassie,
					"Standard1",
					CASSIE_CTOB(key),
					NULL,
					CASSIE_CTOB("age"),
					CASSIE_CONSISTENCY_LEVEL_ONE
					))) {
					retval = 1;
				}
			}
		}
	}

	if (key) free(key);
	if (col) cassie_column_free(col);
	if (cassie) cassie_free(cassie);

	return retval;

}

int main(int argc, char ** argv) {
	unsigned long int i;

	for (i=0;;++i) {
		if (cassie_do_one()) {
			printf(".");
		}
		else {
			printf("X");
		}
		if ((i % 80) == 0) printf("%lu\n", i);
	}

}
