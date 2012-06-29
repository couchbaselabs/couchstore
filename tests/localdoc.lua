package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

function localtest(dbname)
   local key = "a_local_key"
   local value = "the local value"
   local value2 = "an updated value"

   local db = couch.open(dbname, true)

   testlib.assert_no_local_doc(key)

   db:save_local(key, value)
   db:commit()

   testlib.assert_no_doc(key)

   db:close()

   db = couch.open(dbname)

   testlib.check_local_doc(db, key, value)

   db:save_local(key, value2)
   testlib.check_local_doc(db, key, value2)
   db:commit()
   testlib.check_local_doc(db, key, value2)

   db:close()

   db = couch.open(dbname)
   testlib.check_local_doc(db, key, value2)

   db:changes(0, 0, function(di) error("Unexpectedly got a doc: " .. di:id()) end)

   -- Store a non-local document and verify it doesn't collide
   db:save(key, "non local", 1)
   testlib.check_local_doc(db, key, value2)
   testlib.check_doc(db, key, "non local")

   db:delete_local(key)
   db:commit()

   testlib.check_doc(db, key, "non local")
   testlib.assert_no_local_doc(key)

end

testlib.run_test("Local doc test", localtest)
