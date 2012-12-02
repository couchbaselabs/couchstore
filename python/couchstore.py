# couchstore.py
# Python interface to CouchStore library

try:
    from ctypes import *
except ImportError:
    import sys
    cb_path = '/opt/couchbase/lib/python'
    while cb_path in sys.path:
        sys.path.remove(cb_path)
    try:
        from ctypes import *
    except ImportError:
        sys.exit('error: could not import ctypes module')
    else:
        sys.path.insert(0, cb_path)

import errno
import traceback

# Load the couchstore library and customize return types:
try:
    _lib = CDLL("libcouchstore.so")         # Linux
except OSError:
    try:
        _lib = CDLL("libcouchstore.dylib")  # Mac OS
    except OSError:
        try:                                # Windows
            _lib = CDLL("libcouchstore-1.dll")
        except Exception, err:
            traceback.print_exc()
            exit(1)

_lib.couchstore_strerror.restype = c_char_p


class CouchStoreException (Exception):
    """Exceptions raised by CouchStore APIs."""
    def __init__ (self, errcode):
        Exception.__init__(self, _lib.couchstore_strerror(errcode))
        self.code = errcode


### INTERNAL FUNCTIONS:

def _check (err):
    if err == 0:
        return
    elif err == -3:
        raise MemoryError()
    elif err == -5:
        raise KeyError()
    elif err == -11:
        raise OSError(errno.ENOENT)
    else:
        raise CouchStoreException(err)


def _toString (key):
    if not isinstance(key, basestring):
        raise TypeError(key)
    return str(key)


### INTERNAL STRUCTS:

class SizedBuf (Structure):
    _fields_ = [("buf", POINTER(c_char)), ("size", c_size_t)]

    def __init__(self, string):
        if string != None:
            string = _toString(string)
            length = len(string)
            buf = create_string_buffer(string, length)
            Structure.__init__(self, buf, length)
        else:
            Structure.__init__(self, None, 0)

    def __str__(self):
        return string_at(self.buf, self.size)

class DocStruct (Structure):
    _fields_ = [("id", SizedBuf), ("data", SizedBuf)]

class DocInfoStruct (Structure):
    _fields_ = [("id", SizedBuf),
                ("db_seq", c_ulonglong),
                ("rev_seq", c_ulonglong),
                ("rev_meta", SizedBuf),
                ("deleted", c_int),
                ("content_meta", c_ubyte),
                ("bp", c_ulonglong),
                ("size", c_size_t) ]

class LocalDocStruct (Structure):
    _fields_ = [("id", SizedBuf),
                ("json", SizedBuf),
                ("deleted", c_int) ]

class DbInfoStruct (Structure):
    _fields_ = [("filename", c_char_p),
                ("last_sequence", c_ulonglong),
                ("doc_count", c_ulonglong),
                ("deleted_count", c_ulonglong),
                ("space_used", c_ulonglong),
                ("header_position", c_size_t) ]


### DOCUMENT INFO CLASS:

class DocumentInfo (object):
    """Metadata of a document in a CouchStore database."""

    # Values for contentType:
    IS_JSON = 0
    INVALID_JSON = 1
    INVALID_JSON_KEY = 2
    NON_JSON = 3

    def __init__ (self, id):
        self.id = id
        self.deleted = False
        self.contentType = DocumentInfo.NON_JSON
        self.revSequence = 0

    @staticmethod
    def _fromStruct (info, store = None):
        self = DocumentInfo(str(info.id))
        self.store = store
        self.sequence = info.db_seq
        self.revSequence = info.rev_seq
        self.revMeta = str(info.rev_meta)
        self.deleted = (info.deleted != 0)
        self.contentType = info.content_meta & 0x0F
        self.compressed = (info.content_meta & 0x80) != 0
        self._bp = info.bp
        self.physSize = info.size
        return self

    def _asStruct(self):
        struct = DocInfoStruct(SizedBuf(self.id))
        if hasattr(self, "sequence"): struct.db_seq = self.sequence
        if hasattr(self, "revMeta"): struct.rev_meta = SizedBuf(self.revMeta)
        struct.rev_seq = self.revSequence
        struct.deleted = self.deleted
        struct.content_meta = self.contentType & 0x0F
        if hasattr(self, "compressed") and self.compressed:
            struct.content_meta |= 0x80
        if hasattr(self, "_bp"): struct.bp = self._bp
        if hasattr(self, "physSize"): struct.size = self.physSize
        return struct

    def __str__ (self):
        return "DocumentInfo('%s', %d bytes)" % (self.id, self.size)

    def __repr__ (self):
        return "DocumentInfo('%s', %d bytes)" % (self.id, self.size)

    def dump (self):
        return "DocumentInfo('%s', %d bytes, seq=%d, revSeq=%d, deleted=%s, contentType=%d, compressed=%d, bp=%d)" % \
                 (self.id, self.physSize, self.sequence, self.revSequence, self.deleted, \
                  self.contentType, self.compressed, self._bp)

    def getContents(self, options =0):
        """Fetches and returns the contents of a DocumentInfo returned from CouchStore's getInfo
           or getInfoBySequence methods."""
        if not hasattr(self, "store") or not hasattr(self, "_bp"):
            raise Exception("Contents unknown")
        info = self._asStruct()
        docptr = pointer(DocStruct())
        _lib.couchstore_open_doc_with_docinfo(self.store, byref(info), byref(docptr), options)
        contents = str(docptr.contents.data)
        _lib.couchstore_free_document(docptr)
        return contents


### LOCAL-DOCUMENTS CLASS:

class LocalDocs (object):
    """Collection that represents the local documents of a CouchStore."""
    def __init__(self, couchstore):
        self.couchstore = couchstore

    def __getitem__ (self, key):
        """Returns the contents of a local document (as a string) given its ID."""
        id = _toString(key)
        docptr = pointer(LocalDocStruct())
        err = _lib.couchstore_open_local_document(self.couchstore, id, len(id), byref(docptr))
        if err == -5 or (err == 0 and docptr.contents.deleted):
            raise KeyError(id)
        _check(err)
        value = str(docptr.contents.json)
        _lib.couchstore_free_document(docptr)
        return value

    def __setitem__ (self, key, value):
        """Saves a local document with the given ID, or deletes it if the value is None."""
        idbuf = SizedBuf(key)
        doc = LocalDocStruct(idbuf)
        if value != None:
            doc.json = SizedBuf(value)
        else:
            doc.deleted = True
        _check(_lib.couchstore_save_local_document(self.couchstore, byref(doc)))

    def __delitem__ (self, key):
        self.__setitem__(key, None)


### COUCHSTORE CLASS:

class CouchStore (object):
    """Interface to a CouchStore database."""

    def __init__ (self, path, mode =None):
        """Creates a CouchStore at a given path. The option mode parameter can be 'r' for
           read-only access, or 'c' to create the file if it doesn't already exist."""
        if mode == 'r':
            flags = 2 # RDONLY
        elif mode == 'c':
            flags = 1 # CREATE
        else:
            flags = 0

        db = c_void_p()
        _check(_lib.couchstore_open_db(path, c_uint64(flags), byref(db)))
        self._as_parameter_ = db
        self.path = path

    def __del__(self):
        self.close()

    def close (self):
        """Closes the CouchStore."""
        if hasattr(self, "_as_parameter_"):
            _lib.couchstore_close_db(self)
            del self._as_parameter_

    def __str__(self):
        return "CouchStore(%s)" % self.path

    def getDbInfo(self):
        """Returns an object with information about the database.
           Its properties are filename, last_sequence, doc_count, deleted_count, space_used, header_position."""
        info = DbInfoStruct()
        _check(_lib.couchstore_db_info(self, byref(info)))
        return info

    COMPRESS = 1

    def save (self, id, data, options =0):
        """Saves a document with the given ID. Returns the sequence number."""
        if isinstance(id, DocumentInfo):
            infoStruct = id._asStruct()
            idbuf = infoStruct.id
        else:
            idbuf = SizedBuf(id)
            infoStruct = DocInfoStruct(idbuf)
        if data != None:
            doc = DocStruct(idbuf, SizedBuf(data))
            docref = byref(doc)
            if options & CouchStore.COMPRESS:
                infoStruct.content_meta |= 0x80
        else:
            docref = None
        _check(_lib.couchstore_save_document(self, docref, byref(infoStruct), options))
        if isinstance(id, DocumentInfo):
            id.sequence = infoStruct.db_seq
        return infoStruct.db_seq

    def saveMultiple(self, ids, datas, options =0):
        """Saves multiple documents. 'ids' is an array of either strings or DocumentInfo objects.
           'datas' is a parallel array of value strings (or None, in which case the documents
           will be deleted.) Returns an array of new sequence numbers."""
        n = len(ids)
        docStructs = (POINTER(DocStruct) * n)()
        infoStructs = (POINTER(DocInfoStruct) * n)()
        for i in xrange(0, n):
            id = ids[i]
            if isinstance(id, DocumentInfo):
                info = id._asStruct()
            else:
                info = DocInfoStruct(SizedBuf(id))
            doc = DocStruct(info.id)
            if datas and datas[i]:
                doc.data = SizedBuf(datas[i])
            else:
                info.deleted = True
            infoStructs[i] = pointer(info)
            docStructs[i] = pointer(doc)
        _check(_lib.couchstore_save_documents(self, byref(docStructs), byref(infoStructs), n, \
                                              options))
        return [info.contents.db_seq for info in infoStructs]
    pass

    def commit (self):
        """Ensures all saved data is flushed to disk."""
        _check(_lib.couchstore_commit(self))

    DECOMPRESS = 1

    def get (self, id, options =0):
        """Returns the contents of a document (as a string) given its ID."""
        id = _toString(id)
        docptr = pointer(DocStruct())
        err = _lib.couchstore_open_document(self, id, len(id), byref(docptr), options)
        if err == -5:
            raise KeyError(id)
        _check(err)
        data = str(docptr.contents.data)
        _lib.couchstore_free_document(docptr)
        return data

    def __getitem__ (self, key):
        return self.get(key)

    def __setitem__ (self, key, value):
        self.save(key, value)

    def __delitem__ (self, key):
        self.save(key, None)

    # Getting document info:

    def _infoPtrToDoc (self, key, infoptr, err):
        if err == -5:
            raise KeyError(key)
        _check(err)
        info = infoptr.contents
        if info == None:
            return None
        doc = DocumentInfo._fromStruct(info, self)
        _lib.couchstore_free_docinfo(infoptr)
        return doc

    def getInfo (self, id):
        """Returns the DocumentInfo object with the given ID."""
        id = _toString(id)
        infoptr = pointer(DocInfoStruct())
        err = _lib.couchstore_docinfo_by_id(self, id, len(id), byref(infoptr))
        return self._infoPtrToDoc(id, infoptr, err)

    def getInfoBySequence (self, sequence):
        """Returns the DocumentInfo object with the given sequence number."""
        infoptr = pointer(DocInfoStruct())
        err = _lib.couchstore_docinfo_by_sequence(self, c_ulonglong(sequence), byref(infoptr))
        return self._infoPtrToDoc(sequence, infoptr, err)

    # Iterating:

    ITERATORFUNC = CFUNCTYPE(c_int, c_void_p, POINTER(DocInfoStruct), c_void_p)

    def forEachChange(self, since, fn):
        """Calls the function "fn" once for every document sequence since the "since" parameter.
           The single parameter to "fn" will be a DocumentInfo object. You can call
           getContents() on it to get the document contents."""
        def callback (dbPtr, docInfoPtr, context):
            fn(DocumentInfo._fromStruct(docInfoPtr.contents, self))
            return 0
        _check(_lib.couchstore_changes_since(self, since, 0, \
               CouchStore.ITERATORFUNC(callback), c_void_p(0)))

    def changesSince (self, since):
        """Returns an array of DocumentInfo objects, for every document that's changed since the
           sequence number "since"."""
        changes = []
        self.forEachChange(since, lambda docInfo: changes.append(docInfo))
        return changes

    def forEachDoc(self, startKey, endKey, fn):
        def callback (dbPtr, docInfoPtr, context):
            fn(DocumentInfo._fromStruct(docInfoPtr.contents, self))
            return 0

        ids = (SizedBuf * 2)()
        numIDs = 1
        if startKey:
            ids[0] = SizedBuf(startKey)
        if endKey:
            ids[1] = SizedBuf(endKey)
            numIDs = 2
        _check(_lib.couchstore_docinfos_by_id(self, ids, numIDs, 1, \
               CouchStore.ITERATORFUNC(callback), c_void_p(0)))

    @property
    def localDocs(self):
        """A simple dictionary-like object that accesses the CouchStore's local documents."""
        return LocalDocs(self)
