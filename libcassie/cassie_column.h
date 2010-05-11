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

		/* Free a column that was initialized by cassie_column_init */
		void cassie_column_free(cassie_column_t column);

#ifdef __cplusplus
	}

	// Not for public consumption, not in C space:

	/* Initializes a representation of a column (name + value + timestamp)
	 * Call cassie_column_free when done with it
	 * Note that it takes ownership of the given name and value, and the mirrored cassie_blob_free will free them
	 */
	cassie_column_t cassie_column_init(
			cassie_t cassie,
			cassie_blob_t name,
			cassie_blob_t value,
			int64_t timestamp
			);

	cassie_column_t cassie_column_convert(
			cassie_t cassie,
			org::apache::cassandra::Column cpp_column
			);

}
#endif

#endif

