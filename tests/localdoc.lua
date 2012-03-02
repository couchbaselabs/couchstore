package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

function localtest(dbname)
   local key = "a_local_key"
   local value = "the local value"
   local value2 = "an updated value"

   local db = couch.open(dbname, true)

   db:save_local(key, value)
   db:commit()
   db:close()

   db = couch.open(dbname)

   testlib.check_doc(db, key, value)

   db:save_local(key, value2)
   testlib.check_doc(db, key, value2)
   db:commit()
   testlib.check_doc(db, key, value2)

   db:close()

   db = couch.open(dbname)
   testlib.check_doc(db, key, value2)

   db:changes(0, function(db, di) error("Unexpectedly got a doc: " .. di:id()) end)

   db:delete_local(key)
   db:commit()

   local succeeded, result = pcall(db['get_local'], db, key)
   if succeeded then
      error("Unexpectedly read a deleted document.")
   end

end

testlib.run_test("Local doc test", localtest)
