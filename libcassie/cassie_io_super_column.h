/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_IO_SUPER_COLUMN_H
#define __LIBCASSIE_IO_SUPER_COLUMN_H

#include "cassie_types.h"

#ifdef __cplusplus
namespace libcassie {

	extern "C" {
#endif

		cassie_super_column_t cassie_get_super_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_consistency_level_t level
				);

#ifdef __cplusplus
	}
}
#endif

#endif
