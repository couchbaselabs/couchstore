package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

local function check_table(db, t)
   for i,v in ipairs(t) do
      local k = v[1]
      testlib.check_doc(db, k, v[2])
   end

   local found = 0
   db:changes(0, 0, function(di) found = found + 1 end)
   if found ~= #t then
      error("Expected " .. #t .. " results, found " .. found)
   end
end

function test_explicit(dbname)
   local t = {
      {"k1", "value 1", 1,},
      {"k2", "value 2", 1,},
      {"k3", "value 3", 1,},
      {"k4", "value 4", 1,},
      {"k5", "value 5", 1,},
      {"k6", "value 6", 1,},
      {"k7", "value 7", 1,}
   }

   local db = couch.open(dbname, true)

   for i,v in ipairs(t) do
      local k = v[1]
      testlib.assert_no_doc(db, k)
   end

   db:save_bulk(t)
   db:commit()

   check_table(db, t)
end

function large_txn_bulk_test(dbname)
   local t = {}
   for i = 0, 10000, 1 do
      table.insert(t, {"k" .. i, "value " .. i, 1})
   end

   local db = couch.open(dbname, true)

   db:save_bulk(t)
   db:commit()

   check_table(db, t)
end

testlib.run_test("Explicit bulk test", test_explicit)
testlib.run_test("Big bulk test", large_txn_bulk_test)
