local dbname = os.tmpname()

function localtest()
   local key = "a_local_key"
   local value = "the local value"
   local value2 = "an updated value"

   local function check_doc(db, expected_value)
      local doc = db:get_local(key)
      if doc ~= expected_value then
         error(string.format("Expected '%s' (%d bytes), got '%s' (%d bytes)",
                             expected_value, #expected_value, doc, #doc))
      end
   end

   local db = couch.open(dbname, true)

   db:save_local(key, value)
   db:commit()
   db:close()

   db = couch.open(dbname)

   check_doc(db, value)

   db:save_local(key, value2)
   check_doc(db, value2)
   db:commit()
   check_doc(db, value2)

   db:close()

   db = couch.open(dbname)
   check_doc(db, value2)

   db:delete_local(key)
   db:commit()

   local succeeded, result = pcall(db['get_local'], db, key)
   if succeeded then
      error("Unexpectedly read a deleted document.")
   end

end

local succeeded, result = pcall(localtest)

if succeeded then
   print("Local doc test: PASS")
else
   print("Local doc test: FAIL (" .. result .. ")")
end

os.remove(dbname)
