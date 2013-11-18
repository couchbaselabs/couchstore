/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef _MAPREDUCE_VISIBILITY_H
#define _MAPREDUCE_VISIBILITY_H

#if defined(LIBMAPREDUCE_INTERNAL)

#ifdef __SUNPRO_C
#define LIBMAPREDUCE_API __global
#elif defined(HAVE_VISIBILITY) && HAVE_VISIBILITY
#define LIBMAPREDUCE_API __attribute__ ((visibility("default")))
#elif defined(_MSC_VER)
#define LIBMAPREDUCE_API extern __declspec(dllexport)
#else
#define LIBMAPREDUCE_API
#endif

#else

#if defined(_MSC_VER) && !defined(LIBCOUCHSTORE_NO_VISIBILITY)
#define LIBMAPREDUCE_API extern __declspec(dllimport)
#else
#define LIBMAPREDUCE_API
#endif

#endif

#endif
