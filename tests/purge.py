from couchstore import CouchStore, DocumentInfo
from tempfile import mkdtemp
import os
import struct
import unittest

REV_META_PACK = ">QII"

def deleteAt(db, key, time):
    info = db.getInfo(key)
    # cas, exp, flags
    info.revMeta = str(struct.pack(REV_META_PACK, 0, time, 0))
    info.deleted = True
    return db.save(info, "")

class PurgeTest(unittest.TestCase):
    def setUp(self):
        self.tmpdir = mkdtemp()
        self.origname = self.tmpdir + "orig.couch"
        self.purgedname = self.tmpdir + "purged.couch"
        self.origdb = CouchStore(self.origname, 'c');

    def tearDown(self):
        self.origdb.close()
        os.remove(self.origname)
        os.remove(self.purgedname)
        os.rmdir(self.tmpdir)

    def testPurgeCompact(self):
        # Save some docs
        self.origdb.save("foo1", "bar")
        self.origdb.save("foo2", "baz")
        self.origdb.save("foo3", "bell")

        # Delete some
        seqPurged = deleteAt(self.origdb, "foo2", 10)
        seqKept = deleteAt(self.origdb, "foo3", 20)
        self.origdb.commit()

        os.system("./couch_compact --purge-before 15 " + self.origname + " " + self.purgedname)
        self.newdb = CouchStore(self.purgedname)

        # Check purged item is not present in key tree and kept item is
        self.assertRaises(KeyError, self.newdb.getInfo, "foo2")
        self.assertIsNotNone(self.newdb.getInfo("foo3"))

        # Check purged item is not present in seq tree and kept item is
        self.assertRaises(KeyError, self.newdb.getInfoBySequence, seqPurged)
        self.assertIsNotNone(self.newdb.getInfoBySequence(seqKept))


if __name__ == '__main__':
    unittest.main()
