/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_IO_COLUMN_H
#define __LIBCASSIE_IO_COLUMN_H

#include "cassie_types.h"

#ifdef __cplusplus
namespace libcassie {
	extern "C" {
#endif

		/* Insert a column into cassandra
		 * The column will be inserted inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be inserted inside that column_family+super_column_name
		 */
		int cassie_insert_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_blob_t value,
				cassie_consistency_level_t level
				);

		/* Retrieves a column from cassandra
		 * The column will be retrieved from inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be retrieved from that column_family+super_column_name
		 * Call cassie_column_free() when you're done with it
		 */
		cassie_column_t cassie_get_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				);


		/* Similar to cassie_get_column, but will return the columns value as a C String
		 * free() the returned value when you no longer need it
		 * See the notes on the CASSIE_B2C macro regarding lossiness of converting from blob->C string
		 */
		char * cassie_get_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				);

#ifdef __cplusplus
	}
}
#endif

#endif
