package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

function bigdb(dbname)
   local db = couch.open(dbname, true)
   db:truncate(5 * 1024 * 1024 * 1024)
   db:close()

   db = couch.open(dbname)
   testlib.assert_no_doc(db, "k")

   db:save("k", "a value", 1)
   db:commit()
   db:close()

   db = couch.open(dbname)
   testlib.check_doc(db, "k", "a value")
end

testlib.run_test("A Big Database", bigdb)
