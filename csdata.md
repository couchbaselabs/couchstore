## Couchstore Data Types

Struct members not listed should be considered internal to Couchstore.

### sized\_buf
    struct sized_buf {
        char* buf;
        size_t size;
    }

Couchstore uses `sized_buf`s to point to data buffers.

### DocInfo
    struct DocInfo {
        sized_buf id;
        uint64_t db_seq;
        uint64_t rev_seq;
        sized_buf rev_meta;
        int deleted;
        uint8_t content_meta;
    }

* `id` - Document ID
* `db_seq` - Change sequence number the document was inserted at.
* `rev_seq` - The version number of the document
* `rev_meta` - Revision metadata. Used by ep-engine to store CAS value, expiry time, and flags
* `deleted` - 1 if document should be considered "deleted" and not subject to indexing, otherwise 0.
* `content_meta` - Number field used to store flags indicating metadata about the document content (is it JSON, etc.)

#### The `content_meta` field

* The least-significant two bits are used to encode why/whether the body is or is not JSON
    * 0 - Document body *is* JSON
    * 1 - Inserted data was not valid JSON
    * 2 - Inserted data was valid JSON, but contained a key reserved for internal use, such as one with a `$` prefix.
    * 3 - The document was inserted in non-JSON mode.
* If the most-significant bit is set the document body data has been compressed with snappy.

When saving documents with `save_doc` or `save_docs`, `id`, `rev_seq`, `rev_meta`, `deleted`, and `content_meta` must be set on the `DocInfo`s passed to Couchstore. The `db_seq` is determined at insert time.

#### The `rev_meta` field

[128 bit binary value]
| 64 bits  | 32 bits    | 32 bits    |
-----------|------------|------------|
 CAS Value | Expiration | User flags |

 All values are Big Endian.

 * CAS Value - Random nonce value set by Memcached, used to allow ensuring value has not been modified since it was read when writing.
 * Expiration - 32 bit timestamp for value expiry time. 0 if item does not expire.
 * User flags - Memcache API allows users to set this 32 bit number along with values, it is not used by memcached internally.

### Doc
    struct Doc {
        sized_buf id;
        sized_buf data;
    }

`id` contains the document ID, `data` contains the document body data. Couchstore does not compress or modify the body data in any way, unless the COMPRESS_DOC_BODIES or DECOMPRESS_DOC_BODIES flags are passed to the read and write functions, then it will check the `content_meta` of the corresponding doc info to see if it should compress or decompress the doc data.

### LocalDoc
    struct LocalDoc {
        sized_buf id;
        sized_buf json;
        int deleted;
    }

* `id`  - The local document ID, it must start with `_local/`
* `json` - The local document body.
* `deleted` - if set to 1 on a LocalDoc passed to save_local_doc, `json` will be ignored, and the local document with the ID in `id` will be removed, if it exists.


## C API

Documented in `include/libcouchstore/couch_db.h`

