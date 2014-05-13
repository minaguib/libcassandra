/*
 * LibCassie
 * Copyright (C) 2010-2014 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <string.h>

#include <iostream>
#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

#include "cassie.h"
#include "cassie_private.h"

namespace libcassie {

	using namespace std;
	using namespace libcassandra;

	extern "C" {

		cassie_t cassie_init(const char * host, int port) {
			return cassie_init_with_timeout(host, port, -1);
		}

		cassie_t cassie_init_with_timeout(const char * host, int port, int timeout) {

			cassie_t cassie;
			std::tr1::shared_ptr<libcassandra::Cassandra> cassandra;

			if (!host) return(NULL);

			try {
				CassandraFactory factory(host, port, timeout);
				cassandra = factory.create();
			}
			catch (const std::exception& e) {
				//cout << "Exception " << typeid(e).name() << ": " << e.what() << endl;
				return(NULL);
			}

			cassie = new _cassie;
			cassie->host					= strdup(host);
			cassie->port					= port;
			cassie->last_error_string	= NULL;
			cassie->last_error_code		= CASSIE_ERROR_NONE;
			cassie->cassandra				= cassandra;

			return(cassie);
		}

		int cassie_set_keyspace(cassie_t cassie, char * keyspace) {
			try {
				cassie->cassandra->setKeyspace(keyspace);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, CASSIE_ERROR_INVALID_REQUEST, "Exception: %s", ire.why.c_str());
				return(0);
			}
			catch (const std::exception& e) {
				cassie_set_error(cassie, CASSIE_ERROR_OTHER, "Exception %s: %s", typeid(e).name(), e.what());
				return(0);
			}
			return 1;

		}

		void cassie_free(cassie_t cassie) {

			if(!cassie) return;

			if (cassie->host) {
				free(cassie->host);
				cassie->host = NULL;
			}
			cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);

			delete(cassie);

		}

		void cassie_print_debug(cassie_t cassie) {

			try {

				string clus_name= cassie->cassandra->getClusterName();
				cout << "\tcluster name: " << clus_name << endl;

				std::vector<KeyspaceDefinition> key_out= cassie->cassandra->getKeyspaces();
				for (std::vector<KeyspaceDefinition>::iterator it = key_out.begin(); it != key_out.end(); ++it) {
					cout << "\tkeyspace: " << it->getName() << endl;
				}

			}
			catch (const std::exception& e) {
				cout << "Exception caught: " << e.what() << endl;
			}

		}

		void cassie_set_recv_timeout(cassie_t cassie, int timeout) {
			cassie->cassandra->setRecvTimeout(timeout);
		}

		void cassie_set_send_timeout(cassie_t cassie, int timeout) {
			cassie->cassandra->setSendTimeout(timeout);
		}

		int cassie_remove(
				cassie_t cassie,
				const char * column_family,
				cassie_blob_t key,
				cassie_blob_t super_column_name,
				cassie_blob_t column_name,
				cassie_consistency_level_t level
				) {

			org::apache::cassandra::ColumnPath col_path;
			string cpp_key(CASSIE_BDATA(key), CASSIE_BLENGTH(key));

			col_path.column_family.assign(column_family);
			if (super_column_name != NULL) {
				col_path.super_column.assign(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));
				col_path.__isset.super_column = true;
			}
			if (column_name != NULL) {
				col_path.column.assign(CASSIE_BDATA(column_name), CASSIE_BLENGTH(column_name));
				col_path.__isset.column = true;
			}

			try {
				cassie->cassandra->remove(
						cpp_key,
						col_path,
						(org::apache::cassandra::ConsistencyLevel::type) level
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

	} // extern "C"
} // namespace libcassie
