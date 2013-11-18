/*
 * This file is separate from plfs.h for a few reasons (in no particular order):
 * 1. These definitions are used internally in PLFS (where plfs.h is not included) 
 *    as well as externally when using the PLFS API.
 * 2. Users need to be aware of these return codes and their translation from 
 *    PLFS-specific codes into human readable strings and/or generic errnos.
 * 3. By keeping this separate users of the PLFS API will be forced to at least
 *    know of the existence of the PLFS-specific return codes enumerated below.
 */


#ifndef __PLFS_ERROR__
#define __PLFS_ERROR__

#include <errno.h>

#ifdef __cplusplus
extern "C"
{
#endif

typedef enum {
    PLFS_SUCCESS,
    PLFS_EPERM,
    PLFS_ENOENT,
    PLFS_ESRCH,
    PLFS_EINTR,
    PLFS_EIO,
    PLFS_ENXIO,
    PLFS_E2BIG,
    PLFS_EBADF,
    PLFS_EAGAIN,
    PLFS_ENOMEM,
    PLFS_EACCES,
    PLFS_EFAULT,
    PLFS_EBUSY,
    PLFS_EEXIST,
    PLFS_EXDEV,
    PLFS_ENODEV,
    PLFS_ENOTDIR,
    PLFS_EISDIR,
    PLFS_EINVAL,
    PLFS_ENFILE,
    PLFS_EMFILE,
    PLFS_ENOTTY,
    PLFS_ETXTBSY,
    PLFS_EFBIG,
    PLFS_ENOSPC,
    PLFS_ESPIPE,
    PLFS_EROFS,
    PLFS_EMLINK,
    PLFS_EPIPE,
    PLFS_ERANGE,
    PLFS_EDEADLK,
    PLFS_ENAMETOOLONG,
    PLFS_ENOSYS,
    PLFS_ENOTEMPTY,
    PLFS_ELOOP,
    PLFS_ENODATA,
    PLFS_EOVERFLOW,
    PLFS_EBADFD,
    PLFS_EPROTOTYPE,
    PLFS_ENOTSUP,
    PLFS_EEOF,
    PLFS_SIGSEGV,
    PLFS_SIGBUS,
    PLFS_TBD       = 300
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
