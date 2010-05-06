/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <stdlib.h>
#include <libcassandra/cassandra.h>

#include "cassie.h"
#include "cassie_column.h"

namespace libcassie {

	using namespace std;

	extern "C" {

		cassie_column_t cassie_column_init(
				cassie_t cassie,
				const char * name,
				size_t name_len,
				const char * value,
				size_t value_len,
				int64_t timestamp
				) {

			cassie_column_t column = NULL;

			if ((name_len > 0 && name == NULL) || (value_len > 0 && value == NULL)) {
				cassie_set_error(cassie, "Invalid name/value name_len/value_len combination");
				return(NULL);
			}

			column = (cassie_column_t) malloc(sizeof(struct _cassie_column));
			if (!column) {
				cassie_set_error(cassie, "Failed to allocate %u bytes for new struct _cassie_column", sizeof(struct _cassie_column));
				return(NULL);
			}
			column->name_len = column->value_len = column->timestamp = 0;
			column->name = column->value = NULL;

			column->name_len = name_len;
			if (name_len > 0) {
				column->name = (char*) malloc(name_len);
				if (!column->name) {
					cassie_set_error(cassie, "Failed to allocate %u bytes for column name", column->name_len);
					cassie_column_free(column);
					return(NULL);
				}
				memcpy(column->name, name, column->name_len);
			}

			column->value_len = value_len;
			if (value_len > 0) {
				column->value = (char*) malloc(value_len);
				if (!column->value) {
					cassie_set_error(cassie, "Failed to allocate %u bytes for column value", column->value_len);
					cassie_column_free(column);
					return(NULL);
				}
				memcpy(column->value, value, column->value_len);
			}

			column->timestamp = timestamp;

			return(column);

		}

		void cassie_column_free(cassie_column_t column) {
			if (!column) return;
			if (column->name) {
				free(column->name);
				column->name = NULL;
			}
			if (column->value) {
				free(column->value);
				column->value = NULL;
			}
			free(column);
		}

	} // extern "C"


	// Not for public consumption, not in C space:

	cassie_column_t cassie_column_convert(cassie_t cassie, org::apache::cassandra::Column cpp_column) {
		return(
				cassie_column_init(
					cassie,
					cpp_column.name.data(),
					cpp_column.name.length(),
					cpp_column.value.data(),
					cpp_column.value.length(),
					cpp_column.timestamp
					)
				);
	}

} // namespace libcassie
