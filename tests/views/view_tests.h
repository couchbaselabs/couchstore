/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#ifndef _VIEW_TESTS_H
#define _VIEW_TESTS_H

#include "config.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "../macros.h"
#include "../src/views/bitmap.h"
#include "../src/views/sorted_list.h"

#define TPRINT(...) fprintf(stderr, __VA_ARGS__)

void view_tests();
void test_bitmaps();
void test_sorted_lists();

#endif
