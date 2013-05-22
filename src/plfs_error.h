#ifndef __PLFS_ERROR__
#define __PLFS_ERROR__

#include <errno.h>

#ifdef __cplusplus
extern "C"
{
#endif

typedef enum {
    PLFS_SUCCESS    = 0,
    PLFS_EPERM      = 1,
    PLFS_ENOENT     = 2,
    PLFS_ESRCH      = 3,
    PLFS_EINTR      = 4,
    PLFS_EIO        = 5,
    PLFS_ENXIO      = 6,
    PLFS_E2BIG      = 7,
    PLFS_EBADF      = 8,
    PLFS_EAGAIN     = 9,
    PLFS_ENOMEM     = 10,
    PLFS_EACCES     = 11,
    PLFS_EFAULT     = 12,
    PLFS_EBUSY      = 13,
    PLFS_EEXIST     = 14,
    PLFS_EXDEV      = 15,
    PLFS_ENODEV     = 16,
    PLFS_ENOTDIR    = 17,
    PLFS_EISDIR     = 18,
    PLFS_EINVAL     = 19,
    PLFS_ENFILE     = 20,
    PLFS_EMFILE     = 21,
    PLFS_ENOTTY     = 22,
    PLFS_ETXTBSY    = 23,
    PLFS_EFBIG      = 24,
    PLFS_ENOSPC     = 25,
    PLFS_ESPIPE     = 26,
    PLFS_EROFS      = 27,
    PLFS_EMLINK     = 28,
    PLFS_EPIPE      = 29,
    PLFS_ERANGE     = 30,
    PLFS_EDEADLK    = 31,
    PLFS_ENAMETOOLONG = 32,
    PLFS_ENOSYS     = 33,
    PLFS_ENOTEMPTY  = 34,
    PLFS_ELOOP      = 35,
    PLFS_ENODATA    = 36,
    PLFS_EOVERFLOW  = 37,
    PLFS_EBADFD     = 38,
    PLFS_EPROTOTYPE = 39,
    PLFS_ENOTSUP    = 40,
    PLFS_EEOF       = 41,
    PLFS_SIGSEGV    = 42,
    PLFS_SIGBUS     = 43,
    PLFS_TBD        = 300
} plfs_error_t;

#define PLFS_EWOULDBLOCK PLFS_EAGAIN  /*In Posix they are equal */

/* Translate standard errno to plfs error */
plfs_error_t errno_to_plfs_error(int errcode);
/* Translate plfs error to standard errno */
int plfs_error_to_errno(plfs_error_t plfs_err);
/* Return a string describing the meaning of the plfs error code */
const char *strplfserr(plfs_error_t err);

#ifdef __cplusplus
}
#endif

#endif
