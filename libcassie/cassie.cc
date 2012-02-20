/*
 * LibCassie
 * Copyright (C) 2010-2011 Mina Naguib
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

		cassie_t cassie_init_with_timeout_blob(cassie_blob_t host, int port, int timeout) {

			cassie_t cassie;
			std::tr1::shared_ptr<libcassandra::Cassandra> cassandra;

			if (!host) return(NULL);
            string cpp_host(CASSIE_BDATA(host), CASSIE_BLENGTH(host));

			try {
				CassandraFactory factory(cpp_host, port, timeout);
				cassandra = factory.create();
			}
			catch (const std::exception& e) {
				cout << "Exception " << typeid(e).name() << ": " << e.what() << endl;
				return(NULL);
			}

			cassie = new _cassie;
			cassie->host					= CASSIE_BDATA(host);
			cassie->port					= port;
			cassie->last_error_string	= NULL;
			cassie->last_error_code		= CASSIE_ERROR_NONE;
			cassie->cassandra				= cassandra;

			return(cassie);
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

		int cassie_set_keyspace_blob(cassie_t cassie, cassie_blob_t keyspace) {
			try {
	            string cpp_keyspace(CASSIE_BDATA(keyspace), CASSIE_BLENGTH(keyspace));
				cassie->cassandra->setKeyspace(cpp_keyspace);
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

		void cassie_free_blob(cassie_t cassie) {

			if(!cassie) return;
			cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);

			delete(cassie);

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


	} // extern "C"
} // namespace libcassie
