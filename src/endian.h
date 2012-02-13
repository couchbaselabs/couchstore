#ifndef COUCH_ENDIAN_H
#define COUCH_ENDIAN_H
template<typename T> static inline T endianSwap(const T& x)
{
    static const int one = 1;
    static const char sig = *(char*)&one;

    if (sig == 0) return x; // for big endian machine just return the input

    T ret;
    int size = sizeof(T);
    char* src = (char*)&x + sizeof(T) - 1;
    char* dst = (char*)&ret;

    while (size-- > 0) *dst++ = *src--;

    return ret;
}
#endif
