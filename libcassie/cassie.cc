/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include "cassie.h"

#include <string.h>
#include <stdarg.h>
#include <sstream>
#include <iostream>
#include <stdlib.h>
#include <set>
#include <string>
#include <stdio.h>

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

using namespace std;
using namespace libcassandra;

extern "C" {

	struct _cassie {
		char *							host;
		int								port;
		char *							last_error;
		tr1::shared_ptr<Cassandra>	cassandra;
	};

	void _cassie_set_error(cassie_t cassie, const char * format, ...);

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

	char * cassie_last_error(cassie_t cassie) {
		return(cassie->last_error);
	}

	/* PRIVATE */

	void _cassie_set_error(cassie_t cassie, const char * format, ...) {

		va_list ap;

		if (cassie->last_error) {
			free(cassie->last_error);
			cassie->last_error = NULL;
		}

		va_start(ap, format);
		vasprintf(&(cassie->last_error), format, ap);
		va_end(ap);

	}

}
