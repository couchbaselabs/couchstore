#include <v8.h>
#include <stdio.h>

int main(void) {
    int major, minor;
    sscanf(v8::V8::GetVersion(), "%d.%d", &major, &minor);
    if (major >= 3 && minor > 19) {
        fprintf(stderr, "V8_POST_3_19_API");
    } else {
        fprintf(stderr, "V8_PRE_3_19_API");
    }
    return 0;
}
