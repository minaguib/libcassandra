/*
 * LibCassie
 * Copyright (C) 2010-2011 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_H
#define __LIBCASSIE_H

#include <stdint.h>

#ifdef __cplusplus
namespace libcassie {
	extern "C" {
#endif

		/* -----------------------------------------------------
		 * COMMON TYPES
		 * -----------------------------------------------------
		 */

		typedef enum {
			CASSIE_CONSISTENCY_LEVEL_ONE           =  1,
			CASSIE_CONSISTENCY_LEVEL_QUORUM        =  2,
			CASSIE_CONSISTENCY_LEVEL_LOCAL_QUORUM  =  3,
			CASSIE_CONSISTENCY_LEVEL_EACH_QUORUM   =  4,
			CASSIE_CONSISTENCY_LEVEL_ALL           =  5,
			CASSIE_CONSISTENCY_LEVEL_ANY           =  6
		} cassie_consistency_level_t;

		typedef enum {
			CASSIE_ERROR_NONE = 0,
			CASSIE_ERROR_OOM,
			CASSIE_ERROR_TRANSPORT,
			CASSIE_ERROR_INVALID_REQUEST,
			CASSIE_ERROR_OTHER
		} cassie_error_code_t;

		typedef struct _cassie * cassie_t;

		typedef struct _cassie_column * cassie_column_t;

		typedef struct _cassie_super_column * cassie_super_column_t;

		typedef struct _cassie_blob * cassie_blob_t;
		struct _cassie_blob {
			char * data;
			size_t length;
		};

		/* -----------------------------------------------------
		 * In cassie.cc
		 * -----------------------------------------------------
		 */

		/* Initializes a new cassie object
		 * Use cassie_free when done with it
		 */
		cassie_t cassie_init(const char * host, int port);

		/* Initializes a new cassie object with the given
		 * timeout set for connecting, receiving and sending
		 * Use cassie_free when done with it
		 */
		cassie_t cassie_init_with_timeout(const char * host, int port, int timeout);

		/* Set the keyspace you'd like to use */
		int cassie_set_keyspace(cassie_t cassie, char * keyspace);

		/* Frees a cassie object initialied with cassie_init */
		void cassie_free(cassie_t cassie);

		void cassie_print_debug(cassie_t cassie);

		/* Sets the receive timeout in ms */
		void cassie_set_recv_timeout(cassie_t cassie, int timeout);

		/* Sets the send timeout in ms */
		void cassie_set_send_timeout(cassie_t cassie, int timeout);

		/* -----------------------------------------------------
		 * In cassie_error.cc
		 * -----------------------------------------------------
		 */

		char * cassie_last_error_string(cassie_t cassie);
		cassie_error_code_t cassie_last_error_code(cassie_t cassie);


		/* -----------------------------------------------------
		 * In cassie_blob.cc
		 * -----------------------------------------------------
		 */

		/*
		 * Allows in-line conversion of C string to cassie_blob_t
		 *
		 * This helper macro should only be used when it's a parameter to one of the libcassie API functions
		 * You should not use it as a "blob creator".  Use cassie_blob_init instead for that
		 * You should ESPECIALLY not use it in a loop to create blobs and store them in an array
		 */
#define CASSIE_CTOB(cstr) (&(struct _cassie_blob) {(cstr), ((cstr) ? strlen((cstr)) : 0)} )

		/* Returns the underlying data of the blob
		 * Notice that if you treat this as a C string, it will be safe but may be LOSSY:
		 * If the blob already contains a NULL character, your use of it as a C-string will be terminated at that point.
		 * If the blob does not contain a NULL, cassie_blob_init will ensure there's a NULL sitting at its end, so reading it like a C string
		 * will not read past allocation.
		 */
#define CASSIE_BDATA(blob) (cassie_blob_get_data(blob))
		char * cassie_blob_get_data(cassie_blob_t blob);

		/* Returns the underlying length of the blob
		 */
#define CASSIE_BLENGTH(blob) (cassie_blob_get_length(blob))
		size_t cassie_blob_get_length(cassie_blob_t blob);

		/* Initializes a blob
		 * Use cassie_blob_free when you're done with it
		 */
		cassie_blob_t cassie_blob_init(
				const char * data,
				size_t length
				);

		/* Free a blob that was initialized with cassie_blob_init */
		void cassie_blob_free(cassie_blob_t blob);


		/* -----------------
		 * In cassie_column.cc
		 * -----------------
		 */

		/* Free a column that was initialized by cassie_column_init
		 * If the column is part of a linked list, children are freed as well
		 * */
		void cassie_column_free(cassie_column_t column);

		/* Return the column's name */
		cassie_blob_t cassie_column_get_name(cassie_column_t column);

		/* Return the column's name blob data */
		char * cassie_column_get_name_data(cassie_column_t column);

		/* Return the column's value */
		cassie_blob_t cassie_column_get_value(cassie_column_t column);

		/* Return the column's value blob data */
		char * cassie_column_get_value_data(cassie_column_t column);

		/* Return the column's timestamp */
		uint64_t cassie_column_get_timestamp(cassie_column_t column);

		/* Return the next column in case of a linked list */
		cassie_column_t cassie_column_get_next(cassie_column_t column);

		/* -----------------
		 * In cassie_io_column.cc
		 * -----------------
		 */

		/* Insert a column into cassandra
		 * The column will be inserted inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be inserted inside that column_family+super_column_name
		 */
		int cassie_insert_column(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_blob_t value,
				cassie_consistency_level_t level
				);

		/* Insert a column into cassandra with a time to live expiracy
		 * The column will be inserted inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be inserted inside that column_family+super_column_name
		 */
		int cassie_insert_column_ttl(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_blob_t value,
				cassie_consistency_level_t level,
				int ttl
				);

		/* Retrieves a column from cassandra
		 * The column will be retrieved from inside the given column_family
		 * UNLESS super_column_name is specified, in which case it will be retrieved from that column_family+super_column_name
		 * Call cassie_column_free() when you're done with it
		 */
		cassie_column_t cassie_get_column(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
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
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				);

		/* Retrieves multiple columns from cassandra
		 * The columns will be retrieved from inside the given column_family
		 * UNLESS super_column_name is specified, in which case they will be retrieved from that column_family+super_column_name
		 * Call cassie_column_free() on the returned result (first one) when done with them
		 * Use cassie_column_get_next to iterate over them
		 */
		cassie_column_t cassie_get_columns_by_names(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t *column_names,
				cassie_consistency_level_t level
				);

		/* Retrieves multiple columns from cassandra
		 * The columns will be retrieved from inside the given column_family
		 * UNLESS super_column_name is specified, in which case they will be retrieved from that column_family+super_column_name
		 * Call cassie_column_free() on the returned result (first one) when done with them
		 * Use cassie_column_get_next to iterate over them
		 */
		cassie_column_t cassie_get_columns_by_range(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t start_name,
				cassie_blob_t finish_name,
				short int reversed,
				int count,
				cassie_consistency_level_t level
				);

		/* -----------------
		 * In cassie_io_super_column.cc
		 * -----------------
		 */

		/* Retrieves an entire super column from cassandra
		 * Call cassie_super_column_free() when you're done with it
		 */
		cassie_super_column_t cassie_get_super_column(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_consistency_level_t level
				);

		/* Retrieves multiple super columns from cassandra
		 * The columns will be retrieved from inside the given column_family
		 * Call cassie_super_column_free() on the returned result (first one) when done with them
		 * Use cassie_super_column_get_next to iterate over them
		 */
		cassie_super_column_t cassie_get_super_columns_by_names(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t *super_column_names,
				cassie_consistency_level_t level
				);

		/* Retrieves multiple super columns from cassandra
		 * The columns will be retrieved from inside the given column_family
		 * Call cassie_super_column_free() on the returned result (first one) when done with them
		 * Use cassie_super_column_get_next to iterate over them
		 */
		cassie_super_column_t cassie_get_super_columns_by_range(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t start_name,
				cassie_blob_t finish_name,
				short int reversed,
				int count,
				cassie_consistency_level_t level
				);

		/* -----------------
		 * In cassie_super_column.cc
		 * -----------------
		 */

		/* Frees a super column, typically returned with cassie_get_super_column
		 * If the super column is part of a linked list, children are freed as well
		 * */
		void cassie_super_column_free(cassie_super_column_t supercol);

		/* Returns the name of the super column */
		cassie_blob_t cassie_super_column_get_name(cassie_super_column_t supercol);

		/* Return the super column's name blob data */
		char * cassie_super_column_get_name_data(cassie_super_column_t supercol);

		/* Returns the number of columns in the super column */
		unsigned int cassie_super_column_get_num_columns(cassie_super_column_t supercol);

		/* Returns an array of columns in the super column */
		cassie_column_t  * cassie_super_column_get_columns(cassie_super_column_t supercol);

		/* Returns the Nth column in the super column */
		cassie_column_t cassie_super_column_get_column(cassie_super_column_t supercol, unsigned int i);

		/* Return the next super column in case of a linked list */
		cassie_super_column_t cassie_super_column_get_next(cassie_super_column_t super_column);

#ifdef __cplusplus
	}
}
#endif

#endif
