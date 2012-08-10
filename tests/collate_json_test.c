/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

//  Reference: http://wiki.apache.org/couchdb/View_collation

#include "../src/collate_json.h"
#include "macros.h"
#include <string.h>
#include <stdio.h>


void TestCollateJSON(void);


static int collateStrs(const char* str1, const char* str2, CollateJSONMode mode)
{
    // Be evil and put numeric garbage past the ends of str1 and str2, to make sure it
    // doesn't confuse the numeric parsing in the collator:
    size_t len1 = strlen(str1), len2 = strlen(str2);
    char padded1[len1 + 3], padded2[len2 + 3];
    strcpy(padded1, str1);
    strcat(padded1, "99");
    strcpy(padded2, str2);
    strcat(padded2, "88");

    sized_buf buf1 = {padded1, len1};
    sized_buf buf2 = {padded2, len2};
    return CollateJSON(buf1, buf2, mode);
}


static void testEscape(const char* source, char decoded) {
    const char* pos = source;
    assert_eq(ConvertJSONEscape(&pos), decoded);
    assert_eq((size_t)pos, (size_t)(source + strlen(source) - 1));
}


static void TestCollateConvertEscape()
{
    fprintf(stderr, "escapes... ");
    testEscape("\\\\",    '\\');
    testEscape("\\t",     '\t');
    testEscape("\\u0045", 'E');
    testEscape("\\u0001", 1);
    testEscape("\\u0000", 0);
}

static void TestCollateScalars()
{
    fprintf(stderr, "scalars... ");
    CollateJSONMode mode = kCollateJSON_Unicode;
    assert_eq(collateStrs("true", "false", mode), 1);
    assert_eq(collateStrs("false", "true", mode), -1);
    assert_eq(collateStrs("null", "17", mode), -1);
    assert_eq(collateStrs("123", "123", mode), 0);
    assert_eq(collateStrs("123", "1", mode), 1);
    assert_eq(collateStrs("123", "0123.0", mode), 0);
    assert_eq(collateStrs("123", "\"123\"", mode), -1);
    assert_eq(collateStrs("\"1234\"", "\"123\"", mode), 1);
    assert_eq(collateStrs("\"1234\"", "\"1235\"", mode), -1);
    assert_eq(collateStrs("\"1234\"", "\"1234\"", mode), 0);
    assert_eq(collateStrs("\"12\\/34\"", "\"12/34\"", mode), 0);
    assert_eq(collateStrs("\"\\/1234\"", "\"/1234\"", mode), 0);
    assert_eq(collateStrs("\"1234\\/\"", "\"1234/\"", mode), 0);
    assert_eq(collateStrs("\"a\"", "\"A\"", mode), -1);
    assert_eq(collateStrs("\"A\"", "\"aa\"", mode), -1);
    assert_eq(collateStrs("\"B\"", "\"aa\"", mode), 1);
}

static void TestCollateASCII()
{
    fprintf(stderr, "ASCII... ");
    CollateJSONMode mode = kCollateJSON_ASCII;
    assert_eq(collateStrs("true", "false", mode), 1);
    assert_eq(collateStrs("false", "true", mode), -1);
    assert_eq(collateStrs("null", "17", mode), -1);
    assert_eq(collateStrs("123", "1", mode), 1);
    assert_eq(collateStrs("123", "0123.0", mode), 0);
    assert_eq(collateStrs("123", "\"123\"", mode), -1);
    assert_eq(collateStrs("\"1234\"", "\"123\"", mode), 1);
    assert_eq(collateStrs("\"1234\"", "\"1235\"", mode), -1);
    assert_eq(collateStrs("\"1234\"", "\"1234\"", mode), 0);
    assert_eq(collateStrs("\"12\\/34\"", "\"12/34\"", mode), 0);
    assert_eq(collateStrs("\"\\/1234\"", "\"/1234\"", mode), 0);
    assert_eq(collateStrs("\"1234\\/\"", "\"1234/\"", mode), 0);
    assert_eq(collateStrs("\"A\"", "\"a\"", mode), -1);
    assert_eq(collateStrs("\"B\"", "\"a\"", mode), -1);
}

static void TestCollateRaw()
{
    fprintf(stderr, "raw... ");
    CollateJSONMode mode = kCollateJSON_Raw;
    assert_eq(collateStrs("false", "17", mode), 1);
    assert_eq(collateStrs("false", "true", mode), -1);
    assert_eq(collateStrs("null", "true", mode), -1);
    assert_eq(collateStrs("[\"A\"]", "\"A\"", mode), -1);
    assert_eq(collateStrs("\"A\"", "\"a\"", mode), -1);
    assert_eq(collateStrs("[\"b\"]", "[\"b\",\"c\",\"a\"]", mode), -1);
}

static void TestCollateArrays()
{
    fprintf(stderr, "arrays... ");
    CollateJSONMode mode = kCollateJSON_Unicode;
    assert_eq(collateStrs("[]", "\"foo\"", mode), 1);
    assert_eq(collateStrs("[]", "[]", mode), 0);
    assert_eq(collateStrs("[true]", "[true]", mode), 0);
    assert_eq(collateStrs("[false]", "[null]", mode), 1);
    assert_eq(collateStrs("[]", "[null]", mode), -1);
    assert_eq(collateStrs("[123]", "[45]", mode), 1);
    assert_eq(collateStrs("[123]", "[45,67]", mode), 1);
    assert_eq(collateStrs("[123.4,\"wow\"]", "[123.40,789]", mode), 1);
}

static void TestCollateNestedArrays()
{
    fprintf(stderr, "nesting... ");
    CollateJSONMode mode = kCollateJSON_Unicode;
    assert_eq(collateStrs("[[]]", "[]", mode), 1);
    assert_eq(collateStrs("[1,[2,3],4]", "[1,[2,3.1],4,5,6]", mode), -1);
}

static void TestCollateUnicodeStrings()
{
    // Make sure that TDJSON never creates escape sequences we can't parse.
    // That includes "\unnnn" for non-ASCII chars, and "\t", "\b", etc.
    fprintf(stderr, "Unicode... ");
    CollateJSONMode mode = kCollateJSON_Unicode;
    assert_eq(collateStrs("\"fréd\"", "\"fréd\"", mode), 0);
    assert_eq(collateStrs("\"ømø\"",  "\"omo\"", mode), 1);
    assert_eq(collateStrs("\"\t\"",   "\" \"", mode), -1);
    assert_eq(collateStrs("\"\001\"", "\" \"", mode), -1);
}

void TestCollateJSON(void)
{
    fprintf(stderr, "JSON collation: ");
    TestCollateConvertEscape();
    TestCollateScalars();
    TestCollateASCII();
    TestCollateRaw();
    TestCollateArrays();
    TestCollateNestedArrays();
    TestCollateUnicodeStrings();
    fprintf(stderr, "OK\n");

}
