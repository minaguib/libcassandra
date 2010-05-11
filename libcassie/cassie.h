/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_H
#define __LIBCASSIE_H

#include "cassie_types.h"

#ifdef __cplusplus
namespace libcassie {

	extern "C" {
#endif

		/* Initializes a new cassiee object
		 * Use cassie_free when done with it
		 */
		cassie_t cassie_init(const char * host, int port);

		/* Frees a cassie object initialied with cassie_init */
		void cassie_free(cassie_t cassie);

		/* Self-explanatory */
		char * cassie_last_error(cassie_t cassie);
		void cassie_print_debug(cassie_t cassie);

#ifdef __cplusplus
	}

	/* Not for public consumption, not in C space: */
	void cassie_set_error(cassie_t cassie, const char * format, ...);

}
#endif

#endif
