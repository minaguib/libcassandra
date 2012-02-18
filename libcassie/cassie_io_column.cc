/*
 * LibCassie
 * Copyright (C) 2010-2011 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <string.h>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>

#include "cassie.h"
#include "cassie_private.h"

namespace libcassie {

	using namespace std;
	using namespace libcassandra;

	extern "C" {

		int cassie_insert_column(
				cassie_t cassie,
                cassie_blob_t column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_blob_t value,
				cassie_consistency_level_t level
				) {

			string cpp_column_family(CASSIE_BDATA(column_family), CASSIE_BLENGTH(column_family));
			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
			string(cpp_super_column_name);
			if (super_column_name != NULL) {
				cpp_super_column_name.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
			}
			string cpp_column_name(CASSIE_BDATA(column_name), CASSIE_BLENGTH(column_name));
			string cpp_value(CASSIE_BDATA(value), CASSIE_BLENGTH(value));

			try {
				cassie->cassandra->insertColumn(
						cpp_key,
						cpp_column_family,
						cpp_super_column_name,
						cpp_column_name,
						cpp_value,
						(org::apache::cassandra::ConsistencyLevel::type) level,
						0
						);
				cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);
				return(1);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, CASSIE_ERROR_INVALID_REQUEST, "Exception: %s", ire.why.c_str());
				return(0);
			}
			catch (apache::thrift::transport::TTransportException &te) {
				cassie_set_error(cassie, CASSIE_ERROR_TRANSPORT, "Exception: %s: %s", typeid(te).name(), te.what());
				return(0);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, CASSIE_ERROR_OTHER, "Exception %s: %s", typeid(e).name(), e.what());
				return(0);
			}

		}

		cassie_column_t cassie_get_column(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				) {

			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
			string(cpp_super_column_name);
			if (super_column_name  != NULL) {
				cpp_super_column_name.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
			}
			string cpp_column_name(CASSIE_BDATA(column_name), CASSIE_BLENGTH(column_name));

			try {
				org::apache::cassandra::Column cpp_column = cassie->cassandra->getColumn(
						cpp_key,
						column_family,
						cpp_super_column_name,
						cpp_column_name,
						(org::apache::cassandra::ConsistencyLevel::type) level
						);
				return(cassie_column_convert(cassie, cpp_column));
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, CASSIE_ERROR_INVALID_REQUEST, "Exception InvalidRequest: %s", ire.why.c_str());
				return(NULL);
			}
			catch (apache::thrift::transport::TTransportException &te) {
				cassie_set_error(cassie, CASSIE_ERROR_TRANSPORT, "Exception: %s: %s", typeid(te).name(), te.what());
				return(NULL);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, CASSIE_ERROR_OTHER, "Exception %s: %s", typeid(e).name(), e.what());
				return(NULL);
			}

		}

		cassie_column_t cassie_get_columns_by_names(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t *column_names,
				cassie_consistency_level_t level
				) {

			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
			string(cpp_super_column_name);
			vector<string> cpp_column_names;
			cassie_blob_t *blob;
			cassie_column_t result = NULL, previous = NULL, latest = NULL;
			if (super_column_name  != NULL) {
				cpp_super_column_name.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
			}

			for(blob = column_names; *blob != NULL; blob++) {
				string cn(CASSIE_BDATA(*blob), CASSIE_BLENGTH(*blob));
				cpp_column_names.push_back(cn);
			}

			try {
				vector<org::apache::cassandra::Column> cpp_columns = cassie->cassandra->getColumns(
						cpp_key,
						column_family,
						cpp_super_column_name,
						cpp_column_names,
						(org::apache::cassandra::ConsistencyLevel::type) level
						);
				for (vector<org::apache::cassandra::Column>::iterator it = cpp_columns.begin(); it != cpp_columns.end(); ++it) {
					latest = cassie_column_convert(cassie, *it);
					if (result == NULL)
						result = latest;
					else
						previous->next = latest;
					previous = latest;
				}
				return(result);
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, CASSIE_ERROR_INVALID_REQUEST, "Exception InvalidRequest: %s", ire.why.c_str());
				return(NULL);
			}
			catch (apache::thrift::transport::TTransportException &te) {
				cassie_set_error(cassie, CASSIE_ERROR_TRANSPORT, "Exception: %s: %s", typeid(te).name(), te.what());
				return(NULL);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, CASSIE_ERROR_OTHER, "Exception %s: %s", typeid(e).name(), e.what());
				return(NULL);
			}

		}

		char * cassie_get_column_value(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				) {

			char * value = NULL;
			cassie_column_t column = cassie_get_column(cassie, column_family, key, super_column_name, column_name, level);

			if (column) {
				value = strdup(CASSIE_BDATA(column->value));
				cassie_column_free(column);
			}

			return(value);
		}

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
				) {

			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
			string(cpp_super_column_name);
			org::apache::cassandra::SliceRange range;
			cassie_column_t result = NULL, previous = NULL, latest = NULL;
			if (super_column_name  != NULL) {
				cpp_super_column_name.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
			}

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
				vector<org::apache::cassandra::Column> cpp_columns = cassie->cassandra->getColumns(
						cpp_key,
						column_family,
						cpp_super_column_name,
						range,
						(org::apache::cassandra::ConsistencyLevel::type) level
						);
				for (vector<org::apache::cassandra::Column>::iterator it = cpp_columns.begin(); it != cpp_columns.end(); ++it) {
					latest = cassie_column_convert(cassie, *it);
					if (result == NULL)
						result = latest;
					else
						previous->next = latest;
					previous = latest;
				}
				return(result);
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, CASSIE_ERROR_INVALID_REQUEST, "Exception InvalidRequest: %s", ire.why.c_str());
				return(NULL);
			}
			catch (apache::thrift::transport::TTransportException &te) {
				cassie_set_error(cassie, CASSIE_ERROR_TRANSPORT, "Exception: %s: %s", typeid(te).name(), te.what());
				return(NULL);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, CASSIE_ERROR_OTHER, "Exception %s: %s", typeid(e).name(), e.what());
				return(NULL);
			}

		}

	} // extern "C"

} // namespace libcassie
