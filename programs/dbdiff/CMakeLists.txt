ADD_EXECUTABLE(dbdiff dbdiff.c)
TARGET_LINK_LIBRARIES(dbdiff couchstore)

INSTALL(TARGETS dbdiff
        RUNTIME DESTINATION bin)
