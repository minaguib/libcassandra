/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <stdarg.h>
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

			cassie_t cassie;
			std::tr1::shared_ptr<libcassandra::Cassandra> cassandra;

			if (!host) return(NULL);

			try {
				CassandraFactory factory(host, port);
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

		void cassie_free(cassie_t cassie) {

			if(!cassie) return;

			if (cassie->host) {
				free(cassie->host);
				cassie->host = NULL;
			}
			cassie_set_error(cassie, CASSIE_ERROR_NONE, NULL);

			delete(cassie);

		}

		char * cassie_last_error_string(cassie_t cassie) {
			return(cassie->last_error_string);
		}

		cassie_error_code_t cassie_last_error_code(cassie_t cassie) {
			return(cassie->last_error_code);
		}

		void cassie_print_debug(cassie_t cassie) {

			try {

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
			catch (const std::exception& e) {
				cout << "Exception caught: " << e.what() << endl;
			}

		}


	} // extern "C"


	// Not for public consumption, not in C space:

	void cassie_set_error(cassie_t cassie, cassie_error_code_t code, const char * format, ...) {

		va_list ap;
		int i;

		if (cassie->last_error_string) {
			free(cassie->last_error_string);
			cassie->last_error_string = NULL;
		}

		cassie->last_error_code = code;

		if (format != NULL) {
			va_start(ap, format);
			i = vasprintf(&(cassie->last_error_string), format, ap);
			va_end(ap);
		}

	}

} // namespace libcassie
