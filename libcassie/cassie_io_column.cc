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
				cassie_column_t column,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			string(cpp_super_column_name);
			if (super_column_name != NULL) {
				cpp_super_column_name.assign(super_column_name->data, super_column_name->length);
			}
			string cpp_column_name(column->name->data, column->name->length);
			string cpp_value(column->value->data, column->value->length);

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
				cpp_super_column_name.assign(super_column_name->data, super_column_name->length);
			}
			string cpp_column_name(column_name->data, column_name->length);

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


		/* Sugar in case your names and values are all C null-terminated strings */

		int cassie_insert_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				const char * super_column_name,
				const char * column_name,
				const char * value,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;

			try {
				if (super_column_name == NULL) super_column_name = "";
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				key_space->insertColumn(key, column_family, super_column_name, column_name, value);
				return(1);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(0);
			}

		}

		/* Sugar in case your column name and value are valid C-strings */
		char * cassie_get_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				const char * super_column_name,
				const char * column_name,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;

			try {
				if (super_column_name == NULL) super_column_name = "";
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				string res = key_space->getColumnValue(key, column_family, super_column_name, column_name);
				return(strdup(res.c_str()));
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(NULL);
			}
		}

	} // extern "C"

} // namespace libcassie
