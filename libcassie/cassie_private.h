/*
 * LibCassie
 * Copyright (C) 2010-2011 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 *
 */

#ifndef __LIBCASSIE_PRIVATE_H
#define __LIBCASSIE_PRIVATE_H

/* Not for public consumption, not in C space: */

#include <boost/shared_ptr.hpp>

namespace libcassie {

	struct _cassie {
		char *													host;
		int															port;
		cassie_error_code_t							last_error_code;
		char *													last_error_string;
		boost::shared_ptr<libcassandra::Cassandra>	cassandra;
	};

	struct _cassie_column {
		cassie_blob_t name;
		cassie_blob_t value;
		int64_t timestamp;
		struct _cassie_column * next;
	};

	struct _cassie_super_column {
		cassie_blob_t 		name;
		cassie_column_t	* columns;
		unsigned int 		num_columns;
		struct _cassie_super_column * next;
	};

	void cassie_set_error(cassie_t cassie, cassie_error_code_t code, const char * format, ...);

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
