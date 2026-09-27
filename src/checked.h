#ifndef VSR_CHECKED_H
#define VSR_CHECKED_H

#include <stdbool.h>
#include <stddef.h>

/* Internal arena-size arithmetic. out must be non-NULL; unchanged on overflow. */
#if defined(__GNUC__)
#define VSR_SIZE_ATTRIBUTES __attribute__((warn_unused_result, nonnull(3)))
#else
#define VSR_SIZE_ATTRIBUTES
#endif

bool vsr_size_add(size_t a, size_t b, size_t *out) VSR_SIZE_ATTRIBUTES;
bool vsr_size_mul(size_t a, size_t b, size_t *out) VSR_SIZE_ATTRIBUTES;

#undef VSR_SIZE_ATTRIBUTES

#endif /* VSR_CHECKED_H */
