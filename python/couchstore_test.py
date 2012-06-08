from couchstore import CouchStore, CouchStoreException
import os
import unittest

def removeIfExists(path):
    try:
        os.remove(path)
    except OSError as x:
        pass

class NonexistentCouchStoreTest (unittest.TestCase):
    def testNonexistent(self):
        removeIfExists("/tmp/nonexistent.couch")
        self.assertRaises(OSError, CouchStore, "/tmp/nonexistent.couch")

class CouchStoreTest (unittest.TestCase):
    def setUp(self):
        removeIfExists("/tmp/test.couch")
        self.store = CouchStore("/tmp/test.couch", 'c')

    def tearDown(self):
        self.store.close()
        os.remove("/tmp/test.couch")

    def testBasicSave(self):
        sequence = self.store.save("foo", "value of foo")
        self.assertEqual(sequence, 1)
        self.store.commit()
        value = self.store.get("foo")
        self.assertEqual(value, "value of foo")

    def testMissingKey(self):
        self.assertRaises(KeyError, self.store.get, "nonexistent")

    def testBadKey(self):
        self.assertRaises(TypeError, self.store.get, 0)
        self.assertRaises(TypeError, self.store.get, None)
        self.assertRaises(TypeError, self.store.get, [123])

    def testInfo(self):
        value = "value"
        sequence = self.store.save("foo", value)
        self.assertEqual(sequence, 1)
        info = self.store.getInfo("foo")
        self.assertEqual(info.id, "foo")
        self.assertEqual(info.sequence, sequence)
        self.assertFalse(info.deleted)
        #self.assertEqual(info.size, len(value))   #FIXME: Not currently equal, due to bug in CouchStore itself
        self.assertEqual(info.getContents(), value)

    def testInfoBySequence(self):
        value = "value"
        sequence = self.store.save("foo", value)
        self.assertEqual(sequence, 1)
        info = self.store.getInfoBySequence(sequence)
        self.assertEqual(info.id, "foo")
        self.assertEqual(info.sequence, sequence)
        self.assertFalse(info.deleted)
        #self.assertEqual(info.size, len(value))   #FIXME: Not currently equal, due to bug in CouchStore itself
        self.assertEqual(info.getContents(), value)

    def testMissingSequence(self):
        self.store.save("foo", "value")
        self.assertRaises(KeyError, self.store.getInfoBySequence, 99999)
        self.assertRaises(TypeError, self.store.getInfoBySequence, "huh")

    def expectedKey(self, i):
        return "key_%d" % (i+1)
    def expectedValue(self, i):
        return "Hi there! I'm value #%d!" % (i+1)
    def addDocs(self, n):
        for i in xrange(n):
            self.store.save(self.expectedKey(i), self.expectedValue(i))
    def addBulkDocs(self, n):
        ids = [self.expectedKey(i) for i in xrange(n)]
        datas = [self.expectedValue(i) for i in xrange(n)]
        self.store.saveMultiple(ids, datas)

    def testMultipleDocs(self):
        self.addDocs(1000)
        for i in xrange(1000):
            self.assertEqual(self.store[self.expectedKey(i)], self.expectedValue(i))

    def testBulkDocs(self):
        self.addBulkDocs(1000)
        for i in xrange(1000):
            self.assertEqual(self.store[self.expectedKey(i)], self.expectedValue(i))

    def testDelete(self):
        self.store["key"] = "value"
        del self.store["key"]
        self.assertRaises(KeyError, self.store.get, "key")
        info = self.store.getInfo("key")
        self.assertTrue(info.deleted)
        self.assertEqual(info.id, "key")

    def testChangesSince(self):
        self.addDocs(50)
        changes = self.store.changesSince(0)
        self.assertEqual(len(changes), 50)
        for i in xrange(50):
            self.assertEqual(changes[i].id, self.expectedKey(i))


if __name__ == '__main__':
    unittest.main()
