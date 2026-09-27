#ifndef VSR_TEST_CHECK_H
#define VSR_TEST_CHECK_H

#include <stdio.h>
#include <stdlib.h>

/* Test checks must remain active even in a -DNDEBUG build. */
#define CHECK(condition)                                                       \
    do {                                                                       \
        if (!(condition)) {                                                    \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__,   \
                    #condition);                                               \
            abort();                                                           \
        }                                                                      \
    } while (0)

#endif /* VSR_TEST_CHECK_H */
