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

		cassie_t cassie_init(const char * host, int port);
		void cassie_free(cassie_t cassie);

		/* Insert/get when with full blob support */
		int cassie_insert_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_column_t column,
				cassie_consistency_level_t level
				);
		cassie_column_t cassie_get_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				const char * column_name,
				size_t column_name_len,
				cassie_consistency_level_t level
				);

		/* insert/get when you're dealing with C null-terminated strings */
		int cassie_insert_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				const char * column_name,
				const char * value,
				cassie_consistency_level_t level
				);
		char * cassie_get_column_value(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				const char * column_name,
				cassie_consistency_level_t level
				);

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
