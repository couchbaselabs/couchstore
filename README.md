# Couchbase (.couch) storage file C library

## Dependencies:

 * snappy.
 * Lua interpreter, to run the test suite

## How To Build:

 1. `config/autorun.sh`
 2. `./configure`
 3. `make`

Build output library will be in the (invisible) .libs directory -- i.e. `.libs/libcouchstore.dylib` on Mac OS, or `.libs/libcouchstore.so` on Linux.

## Tests:

 1. `make test`

This will run the native tests, and also the Lua tests if Lua was installed at the time the `configure` script ran.

If the tests pass, the output should look something like:

	opening nonexistent file errors... OK
	dump empty db...  OK
	save_doc...  OK
	save_docs...  OK
	local docs...  OK
	compressed bodies...  OK
	changes no dupes...  OK
	PASS: testapp
	==================
	All 1 tests passed
	==================
	Running lua tests.
	Local doc test: PASS
	Simple truncation test: PASS
	Various mangling of headers: PASS
	Explicit bulk test: PASS
	Big bulk test: PASS
