/*
 * LibCassie
 * Copyright (C) 2010 Mina Naguib
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license. See
 * the COPYING file in the parent directory for full text.
 */

#include <libcassandra/cassandra_factory.h>
#include <libcassandra/cassandra.h>
#include <libcassandra/keyspace.h>

#include "cassie.h"
#include "cassie_super_column.h"
#include "cassie_blob.h"
#include "cassie_io_super_column.h"

namespace libcassie {

	using namespace std;
	using namespace libcassandra;

	extern "C" {

		cassie_super_column_t cassie_get_super_column(
				cassie_t cassie,
				const char * keyspace,
				const char * column_family,
				const char * key,
				cassie_blob_t super_column_name,
				cassie_consistency_level_t level
				) {

			Keyspace *key_space;
			string cpp_super_column_name(CASSIE_BDATA(super_column_name), CASSIE_BLENGTH(super_column_name));

			try {
				key_space = cassie->cassandra->getKeyspace(keyspace, (org::apache::cassandra::ConsistencyLevel)level);
				org::apache::cassandra::SuperColumn cpp_super_column = key_space->getSuperColumn(key, column_family, cpp_super_column_name);
				return(cassie_super_column_convert(cassie, cpp_super_column));
			}
			catch (org::apache::cassandra::NotFoundException &nfe) {
				return(NULL);
			}
			catch (org::apache::cassandra::InvalidRequestException &ire) {
				cassie_set_error(cassie, "Exception: %s", ire.why.c_str());
				return(NULL);
			}
		}

	} // extern "C"

} // namespace libcassie
