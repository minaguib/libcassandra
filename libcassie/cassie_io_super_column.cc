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
	using namespace libcassandra;

	extern "C" {

		cassie_super_column_t cassie_get_super_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			string cpp_super_column_name(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				org::apache::cassandra::SuperColumn cpp_super_column = key_space->getSuperColumn(key, column_family, cpp_super_column_name);
				return(cassie_super_column_convert(cassie, cpp_super_column));
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				cassie_set_error(cassie, NULL);
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception InvalidRequest: %s", ire.why.c_str());
				return(NULL);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, "Exception %s: %s", typeid(e).name(), e.what());
				return(NULL);
			}

		}

		cassie_super_column_t cassie_get_super_columns_by_names(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t *super_column_names,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			vector<string> cpp_super_column_names;
			cassie_blob_t *blob;
			cassie_super_column_t result = NULL, previous = NULL, latest = NULL;

			for(blob = super_column_names; *blob != NULL; blob++) {
				string scn(CASSIE_BDATA(*blob), CASSIE_BLENGTH(*blob));
				cpp_super_column_names.push_back(scn);
			}

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				vector<org::apache::cassandra::SuperColumn> cpp_super_columns = key_space->getSuperColumns(key, column_family, cpp_super_column_names);
				for (vector<org::apache::cassandra::SuperColumn>::iterator it = cpp_super_columns.begin(); it != cpp_super_columns.end(); ++it) {
					latest = cassie_super_column_convert(cassie, *it);
					if (result == NULL)
						result = latest;
					else
						previous->next = latest;
					previous = latest;
				}
				return(result);
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				cassie_set_error(cassie, NULL);
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception InvalidRequest: %s", ire.why.c_str());
				return(NULL);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, "Exception %s: %s", typeid(e).name(), e.what());
				return(NULL);
			}

		}

		cassie_super_column_t cassie_get_super_columns_by_range(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t start_name,
				cassie_blob_t finish_name,
				short int reversed,
				int count,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			org::apache::cassandra::SliceRange range;
			cassie_super_column_t result = NULL, previous = NULL, latest = NULL;

			/* Prep range */
			if (start_name != NULL) {
				range.start.assign(CASSIE_BDATA(start_name), CASSIE_BLENGTH(start_name));
			}
			if (finish_name != NULL) {
				range.finish.assign(CASSIE_BDATA(finish_name), CASSIE_BLENGTH(finish_name));
			}
			if (reversed) {
				range.reversed = true;
			}
			if (count != 0) {
				range.count = count;
			}

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				vector<org::apache::cassandra::SuperColumn> cpp_super_columns = key_space->getSuperColumns(key, column_family, range);
				for (vector<org::apache::cassandra::SuperColumn>::iterator it = cpp_super_columns.begin(); it != cpp_super_columns.end(); ++it) {
					latest = cassie_super_column_convert(cassie, *it);
					if (result == NULL)
						result = latest;
					else
						previous->next = latest;
					previous = latest;
				}
				return(result);
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				cassie_set_error(cassie, NULL);
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception InvalidRequest: %s", ire.why.c_str());
				return(NULL);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, "Exception %s: %s", typeid(e).name(), e.what());
				return(NULL);
			}

		}

	} // extern "C"

} // namespace libcassie
