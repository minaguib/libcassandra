/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_BLOB_H
#define __LIBCASSIE_BLOB_H

#include "cassie_types.h"

#ifdef __cplusplus
namespace libcassie {

	extern "C" {
#endif

		cassie_blob_t cassie_blob_init(
				const char * data,
				size_t length
				);

		void cassie_blob_free(cassie_blob_t blob);

#ifdef __cplusplus
	}
}
#endif

#endif

