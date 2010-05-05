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

#ifdef __cplusplus
extern "C" {
#endif

	typedef struct _cassie * cassie_t;

	typedef enum {
		CASSIE_CONSISTENCY_LEVEL_ZERO          =  0,
		CASSIE_CONSISTENCY_LEVEL_ONE           =  1,
		CASSIE_CONSISTENCY_LEVEL_QUORUM        =  2,
		CASSIE_CONSISTENCY_LEVEL_DCQUORUM      =  3,
		CASSIE_CONSISTENCY_LEVEL_DCQUORUMSYNC  =  4,
		CASSIE_CONSISTENCY_LEVEL_ALL           =  5,
		CASSIE_CONSISTENCY_LEVEL_ANY           =  6
	} cassie_consistency_level_t;

	cassie_t cassie_init(const char * host, int port);
	void cassie_free(cassie_t cassie);

	void cassie_print_debug(cassie_t cassie);
	char * cassie_last_error(cassie_t cassie);

	int cassie_insert_column(
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

#ifdef __cplusplus
}
#endif

#endif
