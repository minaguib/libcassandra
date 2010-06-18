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
#include "cassie_private.h"

namespace libcassie {

	using namespace std;

	extern "C" {

		void cassie_super_column_free(cassie_super_column_t supercol) {

			unsigned int i;

			if (!supercol) return;

			if (supercol->next) {
				cassie_super_column_free(supercol->next);
				supercol->next = NULL;
			}

			if (supercol->name) {
				cassie_blob_free(supercol->name);
				supercol->name = NULL;
			}

			if (supercol->columns) {
				for (i=0; i < supercol->num_columns; i++) {
					if (supercol->columns[i] != NULL) {
						cassie_column_free(supercol->columns[i]);
						supercol->columns[i] = NULL;
					}
				}
			}
			free(supercol->columns);
			supercol->columns = NULL;
			supercol->num_columns = 0;

			free(supercol);
		}

		cassie_blob_t cassie_super_column_get_name(cassie_super_column_t supercol) {
			return(supercol->name);
		}

		char * cassie_super_column_get_name_data(cassie_super_column_t supercol) {
			return(CASSIE_BDATA(supercol->name));
		}

		unsigned int cassie_super_column_get_num_columns(cassie_super_column_t supercol) {
			return(supercol->num_columns);
		}

		cassie_column_t  * cassie_super_column_get_columns(cassie_super_column_t supercol) {
			return(supercol->columns);
		}

		cassie_column_t cassie_super_column_get_column(cassie_super_column_t supercol, unsigned int i) {
			if (i < supercol->num_columns) {
				return(supercol->columns[i]);
			}
			else {
				return(NULL);
			}
		}

		cassie_super_column_t cassie_super_column_get_next(cassie_super_column_t super_column) {
			return(super_column->next);
		}

	} // extern "C"

	// Not for public consumption, not in C space:

	cassie_super_column_t cassie_super_column_convert(cassie_t cassie, org::apache::cassandra::SuperColumn cpp_super_column) {

		cassie_super_column_t supercol;
		int i, j;

		supercol = (cassie_super_column_t)malloc(sizeof(struct _cassie_super_column));
		if (!supercol) {
			cassie_set_error(cassie, "Failed to allocate memory for a super_column_t");
			return(NULL);
		}
		supercol->name = NULL;
		supercol->columns = NULL;
		supercol->num_columns = 0;
		supercol->next = NULL;

		supercol->name = cassie_blob_init(cpp_super_column.name.data(), cpp_super_column.name.length());
		if (!supercol->name) {
			cassie_set_error(cassie, "Failed to allocate memory for super column name");
			cassie_super_column_free(supercol);
			return(NULL);
		}

		i = cpp_super_column.columns.size();
		supercol->columns = (cassie_column_t *)malloc(sizeof(struct _cassie_column) * i);
		if (!supercol->columns) {
			cassie_set_error(cassie, "Failed to allocate memory for super column's columns");
			cassie_super_column_free(supercol);
			return(NULL);
		}

		for (j=0; j < i; j++) {
			supercol->columns[j] = cassie_column_convert(cassie, cpp_super_column.columns[j]);
			supercol->num_columns = j + 1;
		}

		return(supercol);
	}

} // namespace libcassie
