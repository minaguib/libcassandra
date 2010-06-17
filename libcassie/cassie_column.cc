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
#include "cassie_private.h"

namespace libcassie {

	using namespace std;

	extern "C" {

		void cassie_column_free(cassie_column_t column) {
			if (!column) return;
			if (column->next) {
				cassie_column_free(column->next);
				column->next = NULL;
			}
			if (column->name) {
				cassie_blob_free(column->name);
				column->name = NULL;
			}
			if (column->value) {
				cassie_blob_free(column->value);
				column->value = NULL;
			}
			free(column);
		}

		cassie_blob_t cassie_column_get_name(cassie_column_t column) {
			return(column->name);
		}

		char * cassie_column_get_name_data(cassie_column_t column) {
			return(CASSIE_BDATA(column->name));
		}

		cassie_blob_t cassie_column_get_value(cassie_column_t column) {
			return(column->value);
		}

		char * cassie_column_get_value_data(cassie_column_t column) {
			return(CASSIE_BDATA(column->value));
		}

		uint64_t cassie_column_get_timestamp(cassie_column_t column) {
			return(column->timestamp);
		}

		cassie_column_t cassie_column_get_next(cassie_column_t column) {
			return(column->next);
		}

	} // extern "C"


	// Not for public consumption, not in C space:

	cassie_column_t cassie_column_init(
			cassie_t cassie,
			cassie_blob_t name,
			cassie_blob_t value,
			int64_t timestamp
			) {

		cassie_column_t column = NULL;

		column = (cassie_column_t) malloc(sizeof(struct _cassie_column));
		if (!column) {
			cassie_set_error(cassie, "Failed to allocate %u bytes for new struct _cassie_column", sizeof(struct _cassie_column));
			return(NULL);
		}

		column->name = name;
		column->value = value;
		column->timestamp = timestamp;
		column->next = NULL;

		return(column);

	}

	cassie_column_t cassie_column_convert(cassie_t cassie, org::apache::cassandra::Column cpp_column) {
		cassie_blob_t name = cassie_blob_init(cpp_column.name.data(), cpp_column.name.length());
		cassie_blob_t value = cassie_blob_init(cpp_column.value.data(), cpp_column.value.length());
		cassie_column_t column = cassie_column_init(
				cassie,
				name,
				value,
				cpp_column.timestamp
				);
		return(column);
	}

} // namespace libcassie
