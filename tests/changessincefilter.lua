package.path = package.path .. ";tests/?.lua"
local testlib = require("testlib")

function test_filter_deletes(dbname)
    local t = {
       {"k1", "value 1", 1,},
       {"k2", "value 2", 1,},
       {"k3", "value 3", 1,},
       {"k4", "value 4", 1,},
       {"k5", "value 5", 1,}
    }

    local db = couch.open(dbname, true)

    for i,v in ipairs(t) do
        local k = v[1]
        testlib.assert_no_doc(db, k)
    end

    db:save_bulk(t)
    db:commit()

    db:delete("k1")
    db:delete("k2")
    db:commit()

    local found = 0
    db:changes(0, 4, function(di) found = found + 1 end)
    if found ~= 3 then
        error("Expected " .. 3 .. " results, found " .. found)
    end

    db:close()
end

function test_filter_mutations(dbname)
    local t = {
       {"k1", "value 1", 1,},
       {"k2", "value 2", 1,},
       {"k3", "value 3", 1,},
       {"k4", "value 4", 1,},
       {"k5", "value 5", 1,}
    }

    local db = couch.open(dbname, true)

    for i,v in ipairs(t) do
        local k = v[1]
        testlib.assert_no_doc(db, k)
    end

    db:save_bulk(t)
    db:commit()

    db:delete("k1")
    db:delete("k2")
    db:commit()

    local found = 0
    db:changes(0, 2, function(di) found = found + 1 end)
    if found ~= 2 then
        error("Expected " .. 2 .. " results, found " .. found)
    end

    db:close()
end

testlib.run_test("Filter deletes", test_filter_deletes)
testlib.run_test("Filter mutations", test_filter_mutations)
