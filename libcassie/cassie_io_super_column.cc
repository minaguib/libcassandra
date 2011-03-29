/*
 * LibCassie
 * Copyright (C) 2010-2011 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>

#include "cassie.h"
#include "cassie_private.h"

namespace libcassie {

	using namespace std;
	using namespace libcassandra;

	extern "C" {

		cassie_super_column_t cassie_get_super_column(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_consistency_level_t level
				) {

			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
			string cpp_super_column_name(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));

			try {
				org::apache::cassandra::SuperColumn cpp_super_column = cassie->cassandra->getSuperColumn(
						cpp_key,
						column_family,
						cpp_super_column_name,
						(org::apache::cassandra::ConsistencyLevel::type) level
						);
				return(cassie_super_column_convert(cassie, cpp_super_column));
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

		cassie_super_column_t cassie_get_super_columns_by_names(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t *super_column_names,
				cassie_consistency_level_t level
				) {

			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
			vector<string> cpp_super_column_names;
			cassie_blob_t *blob;
			cassie_super_column_t result = NULL, previous = NULL, latest = NULL;

			for(blob = super_column_names; *blob != NULL; blob++) {
				string scn(CASSIE_BDATA(*blob), CASSIE_BLENGTH(*blob));
				cpp_super_column_names.push_back(scn);
			}

			try {
				vector<org::apache::cassandra::SuperColumn> cpp_super_columns = cassie->cassandra->getSuperColumns(
						cpp_key,
						column_family,
						cpp_super_column_names,
						(org::apache::cassandra::ConsistencyLevel::type) level
						);
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

		cassie_super_column_t cassie_get_super_columns_by_range(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t start_name,
				cassie_blob_t finish_name,
				short int reversed,
				int count,
				cassie_consistency_level_t level
				) {

			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));
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
				vector<org::apache::cassandra::SuperColumn> cpp_super_columns = cassie->cassandra->getSuperColumns(
						cpp_key,
						column_family,
						range,
						(org::apache::cassandra::ConsistencyLevel::type) level
						);
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
