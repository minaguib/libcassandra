/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_PRIVATE_H
#define __LIBCASSIE_PRIVATE_H

/* Not for public consumption, not in C space: */

namespace libcassie {

	struct _cassie {
		char *														host;
		int															port;
		char *														last_error;
		std::tr1::shared_ptr<libcassandra::Cassandra>	cassandra;
	};

	void cassie_set_error(cassie_t cassie, const char * format, ...);

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

	cassie_super_column_t cassie_super_column_convert(
			cassie_t cassie,
			org::apache::cassandra::SuperColumn cpp_super_column
			);

}

#endif
