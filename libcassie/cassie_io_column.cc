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
#include "cassie_column.h"
#include "cassie_blob.h"
#include "cassie_io_column.h"

namespace libcassie {

	using namespace std;
	using namespace libcassandra;

	extern "C" {

		int cassie_insert_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_blob_t value,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			string(cpp_super_column_name);
			if (super_column_name != NULL) {
				cpp_super_column_name.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
			}
			string cpp_column_name(CASSIE_BDATA(column_name), CASSIE_BLENGTH(column_name));
			string cpp_value(CASSIE_BDATA(value), CASSIE_BLENGTH(value));

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				key_space->insertColumn(key, column_family, cpp_super_column_name, cpp_column_name, cpp_value);
				return(1);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(0);
			}

		}

		cassie_column_t cassie_get_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			string(cpp_super_column_name);
			if (super_column_name  != NULL) {
				cpp_super_column_name.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
			}
			string cpp_column_name(CASSIE_BDATA(column_name), CASSIE_BLENGTH(column_name));

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				org::apache::cassandra::Column cpp_column = key_space->getColumn(key, column_family, cpp_super_column_name, cpp_column_name);
				return(cassie_column_convert(cassie, cpp_column));
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(NULL);
			}
		}

		char * cassie_get_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				) {

			char * value = NULL;
			cassie_column_t column = cassie_get_column(cassie, keyspace, column_family, key, super_column_name, column_name, level);

			if (column) {
				value = strdup(CASSIE_BDATA(column->value));
				cassie_column_free(column);
			}

			return(value);
		}

	} // extern "C"

} // namespace libcassie
