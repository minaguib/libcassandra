/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <string.h>
#include <stdarg.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <stdio.h>

#include "cassie.h"

#include <set>
#include <string>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

using namespace std;
using namespace libcassandra;

namespace libcassie {

	/* Private */
	void cassie_set_error(cassie_t cassie, const char * format, ...);
	cassie_column_t cassie_column_convert(cassie_t cassie, org::apache::cassandra::Column cpp_column);

	extern "C" {

		struct _cassie {
			char *							host;
			int								port;
			char *							last_error;
			tr1::shared_ptr<Cassandra>	cassandra;
		};

		cassie_t cassie_init(const char * host, int port) {

			cassie_t cassie;

			if (!host) return(NULL);

			cassie = new _cassie;
			cassie->host 			= strdup(host);
			cassie->port 			= port;
			cassie->last_error	= NULL;

			CassandraFactory factory(host, port);
			cassie->cassandra = factory.create();

			return(cassie);
		}

		void cassie_free(cassie_t cassie) {

			if(!cassie) return;

			if (cassie->host) {
				free(cassie->host);
			}
			if (cassie->last_error) {
				free(cassie->last_error);
			}
			delete(cassie);

		}

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

		int cassie_insert_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_column_t column,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			string column_name(column->name, column->name_len);
			string value(column->value, column->value_len);

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				key_space->insertColumn(key, column_family, column_name, value);
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
				const char * column_name,
				size_t column_name_len,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				string cn(column_name, column_name_len);
				org::apache::cassandra::Column cpp_column = key_space->getColumn(key, column_family, cn);
				return(cassie_column_convert(cassie, cpp_column));
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(NULL);
			}
		}


		/* Sugar in case your column name and value are valid C-strings */
		int cassie_insert_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				const char * column_name,
				const char * value,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				key_space->insertColumn(key, column_family, column_name, value);
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
				const char * column_name,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				string res = key_space->getColumnValue(key, column_family, column_name);
				return(strdup(res.c_str()));
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(NULL);
			}
		}

		char * cassie_last_error(cassie_t cassie) {
			return(cassie->last_error);
		}

		void cassie_print_debug(cassie_t cassie) {

			string clus_name= cassie->cassandra->getClusterName();
			cout << "\tcluster name: " << clus_name << endl;

			set<string> key_out= cassie->cassandra->getKeyspaces();
			for (set<string>::iterator it = key_out.begin(); it != key_out.end(); ++it) {
				cout << "\tkeyspace: " << *it << endl;
			}

			map<string, string> tokens= cassie->cassandra->getTokenMap(false);
			for (map<string, string>::iterator it= tokens.begin();
					it != tokens.end();
					++it)
			{
				cout << "\t" << it->first << " : " << it->second << endl;
			}
		}



	} // extern "C"


	// Private - not accessible from C-land

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

	void cassie_set_error(cassie_t cassie, const char * format, ...) {

		va_list ap;

		if (cassie->last_error) {
			free(cassie->last_error);
			cassie->last_error = NULL;
		}

		va_start(ap, format);
		vasprintf(&(cassie->last_error), format, ap);
		va_end(ap);

	}

} // namespace libcassie
