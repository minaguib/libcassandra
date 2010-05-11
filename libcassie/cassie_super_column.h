/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_SUPER_COLUMN_H
#define __LIBCASSIE_SUPER_COLUMN_H

#include "cassie_types.h"

#ifdef __cplusplus
namespace libcassie {
	extern "C" {
#endif

		/* Frees a super column, typically returned with cassie_get_super_column */
		void cassie_super_column_free(cassie_super_column_t supercol);

#ifdef __cplusplus
	}

	// Not for public consumption, not in C space:
	cassie_super_column_t cassie_super_column_convert(
			cassie_t cassie,
			org::apache::cassandra::SuperColumn cpp_super_column
			);

}
#endif

#endif

