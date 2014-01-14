# Couchbase (.couch) storage file C library

Currently this library can only be built as part of Couchbase Server due to dependencies on the [Couchbase Platform Library][plat] and [Couchbase Server CMake project][tlm]. For instructions on building Couchbase Server, see the [Manifest Repository][manifest].

[plat]: https://github.com/couchbase/platform
[tlm]: https://github.com/couchbase/tlm
[manifest]: https://github.com/couchbase/manifest

## Tests:

 1. `make test`

This will run the native tests, and also the Lua tests if Lua was installed at the time the `configure` script ran.

Tests use the CMake CTest system, and the [ctest](http://www.cmake.org/cmake/help/v2.8.8/ctest.html) command can be used to run invidual tests and print verbose output.
