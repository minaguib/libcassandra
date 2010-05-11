/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

#include "cassie.h"
#include "cassie_blob.h"

namespace libcassie {

	using namespace std;

	extern "C" {

		cassie_blob_t cassie_blob_init(
				const char * data,
				size_t length
				) {
			cassie_blob_t blob = NULL;

			if (length > 0 && !data) return(NULL);
			blob = (cassie_blob_t)malloc(sizeof(struct _cassie_blob));
			if(!blob) return(NULL);

			blob->length = length;
			if (length > 0) {
				blob->data = (char*)malloc(length + 1);
				if (!(blob->data)) {
					cassie_blob_free(blob);
					return(NULL);
				}
				memcpy(blob->data, data, length);
				blob->data[length] = 0;
			}

			return(blob);
		}

		void cassie_blob_free(cassie_blob_t blob) {
			if (blob->data) {
				free(blob->data);
				blob->data = NULL;
			}
			free(blob);
		}


	} // extern "C"
} // namespace libcassie
