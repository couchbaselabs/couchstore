package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

local alphabet_str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
local alphabet = {}
for i = 1, #alphabet_str do
   alphabet[i] = alphabet_str:sub(i, i)
end


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

function make_val(size)
   local chars = {}
   for i = 1, size do
      chars[i] = alphabet[math.random(1, #alphabet)]
   end
   return table.concat(chars)
end

function test_big_bulk(dbname)
   print "Making data set..."
   local t = {}
   for i = 0, 40000, 5 do
      table.insert(t, {"k" .. i, make_val(i), 1})
   end

   print "Saving data set..."
   local db = couch.open(dbname, true)
   db:save_bulk(t)
   db:commit()

   print "Checking data set..."
   check_table(db, t)

   db:close()

   local db = couch.open(dbname)

   check_table(db, t)
end

function test_big_sequential(dbname)
   print "Making & saving data set..."
   local db = couch.open(dbname, true)

   local t = {}
   for i = 0, 40000, 5 do
      local k = "k" .. i
      local v = make_val(i)
      table.insert(t, {k, v, 1})
      db:save(k, v, 1)
   end

   db:commit()

   print "Checking data set..."
   check_table(db, t)

   db:close()

   local db = couch.open(dbname)

   check_table(db, t)
end

testlib.run_test("Big item test bulk", test_big_bulk)
testlib.run_test("Big item test sequential", test_big_sequential)
