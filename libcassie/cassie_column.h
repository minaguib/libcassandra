/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_COLUMN_H
#define __LIBCASSIE_COLUMN_H

#include "cassie_types.h"

#ifdef __cplusplus
namespace libcassie {

	extern "C" {
#endif

		cassie_column_t cassie_column_init(
				cassie_t cassie,
				const char * name,
				size_t name_len,
				const char * value,
				size_t value_len,
				int64_t timestamp
				);
		void cassie_column_free(cassie_column_t column);

#ifdef __cplusplus
	}

	// Not for public consumption, not in C space:
	cassie_column_t cassie_column_convert(cassie_t cassie, org::apache::cassandra::Column cpp_column);

}
#endif

#endif

