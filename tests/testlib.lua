local M = {}


-- Verify a doc with the given key exists with the given value in the
-- given database.
function M.check_doc(db, key, value)
   local doc = db:get(key)
   if doc ~= value then
      error(string.format("Expected '%s' (%d bytes), got '%s' (%d bytes)",
                          value, #value, doc, #doc))
   end
end

-- Verify a local doc with the given key exists with the given value
-- in the given database.
function M.check_local_doc(db, key, value)
   local doc = db:get_local(key)
   if doc ~= value then
      error(string.format("Expected '%s' (%d bytes), got '%s' (%d bytes)",
                          value, #value, doc, #doc))
   end
end

-- Verify a document does not exist.
function M.assert_no_doc(db, key)
   local succeded, result = pcall(db['get'], db, key)
   if succeeded then
      error("Expected missing doc for " .. key .. " got " .. result)
   end
end

-- Verify a local doc does not exist.
function M.assert_no_local_doc(db, key)
   local succeded, result = pcall(db['get_local'], db, key)
   if succeeded then
      error("Expected missing local doc for " .. key .. " got " .. result)
   end
end

-- Run a named test function.
--
-- The function will receive a filename it should use for the
-- database.  The file will be removed at the end of the test run.  If
-- the test function runs without error, the test is considered
-- successful.
function M.run_test(name, fun)
   local dbname = os.tmpname()

   local succeeded, result = pcall(fun, dbname)

   os.remove(dbname)

   if succeeded then
      print(name .. ": PASS")
   else
      print(name .. ": FAIL (" .. result .. ")")
   end
end

return M
