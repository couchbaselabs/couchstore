#ifndef WIN32_H
#define WIN32_H 1

#ifndef __WIN32__
#define __WIN32__
#endif
// Including SDKDDKVer.h defines the highest available Windows platform.

// If you wish to build your application for a previous Windows platform, include WinSDKVer.h and
// set the _WIN32_WINNT macro to the platform you wish to support before including SDKDDKVer.h.

#include <SDKDDKVer.h>

#define WIN32_LEAN_AND_MEAN             // Exclude rarely-used stuff from Windows headers
#define MAXPATHLEN 1024
#include <windows.h>
#include <stdio.h>
#include <stdint.h>
#include <io.h>
#include <errno.h>                    // for errno definition
#include <winsock2.h>                 // for ntohl

#define inline __inline

#define ssize_t long
#define off_t   long

ssize_t pread(int fd, void *buf, size_t count, off_t offset);
ssize_t pwrite(int fd, const void *buf, size_t nbytes, off_t offset);
int fsync(int fd);
#endif
