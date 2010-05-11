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

		/* Allows in-line conversion of C string to cassie_blob_t */
#define CASSIE_CTOB(cstr) (cstr == NULL ? (&(struct _cassie_blob){NULL, 0}) : (&(struct _cassie_blob){(cstr), strlen(cstr)}))

		/* Returns the underlying data of the blob
		 * Notice that if you treat this as a C string, it will be safe but may be LOSSY:
		 * If the blob already contains a NULL character, your use of it as a C-string will be terminated at that point.
		 * If the blob does not contain a NULL, cassie_blob_init will ensure there's a NULL sitting at its end, so reading it like a C string
		 * will not read past allocation.
		 */
#define CASSIE_BDATA(blob) (blob->data)

		/* Returns the underlying length of the blob
		 */
#define CASSIE_BLENGTH(blob) (blob->length)

		/* Initializes a blob
		 * Use cassie_blob_free when you're done with it
		 */
		cassie_blob_t cassie_blob_init(
				const char * data,
				size_t length
				);

		/* Free a blob that was initialized with cassie_blob_init */
		void cassie_blob_free(cassie_blob_t blob);

#ifdef __cplusplus
	}
}
#endif

#endif

