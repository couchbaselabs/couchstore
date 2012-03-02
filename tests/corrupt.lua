package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

function simpletrunc(dbname)
   local db = couch.open(dbname, true)
   db:save("k", "original value", 1)
   db:commit()

   testlib.check_doc(db, "k", "original value")

   db:save("k", "second value", 1)
   db:commit()

   testlib.check_doc(db, "k", "second value")

   db:truncate(-10)
   db:close()

   db = couch.open(dbname)
   testlib.check_doc(db, "k", "original value")

   db:save("k", "third value", 1)
   db:commit()
   db:close()

   db = couch.open(dbname)
   testlib.check_doc(db, "k", "third value")
end

testlib.run_test("Simple truncation test", simpletrunc)
