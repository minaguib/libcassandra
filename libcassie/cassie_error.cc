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

	extern "C" {

		char * cassie_last_error_string(cassie_t cassie) {
			return(cassie->last_error_string);
		}

		cassie_error_code_t cassie_last_error_code(cassie_t cassie) {
			return(cassie->last_error_code);
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

