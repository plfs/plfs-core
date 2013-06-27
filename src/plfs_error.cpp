#include "plfs_error.h"

plfs_error_t errno_to_plfs_error(int errcode) {
    switch (errcode) {
    case 0:
        return PLFS_SUCCESS;
    case EPERM:
        return PLFS_EPERM;
    case ENOENT:
        return PLFS_ENOENT;
    case ESRCH:
        return PLFS_ESRCH;
    case EINTR:
        return PLFS_EINTR;
    case EIO:
        return PLFS_EIO;
    case ENXIO:
        return PLFS_ENXIO;
    case E2BIG:
        return PLFS_E2BIG;
    case EBADF:
        return PLFS_EBADF;
    case EAGAIN:
        return PLFS_EAGAIN;
    case ENOMEM:
        return PLFS_ENOMEM;
    case EACCES:
        return PLFS_EACCES;
    case EFAULT:
        return PLFS_EFAULT;
    case EBUSY:
        return PLFS_EBUSY;
    case EEXIST:
        return PLFS_EEXIST;
    case EXDEV:
        return PLFS_EXDEV;
    case ENODEV:
        return PLFS_ENODEV;
    case ENOTDIR:
        return PLFS_ENOTDIR;
    case EISDIR:
        return PLFS_EISDIR;
    case EINVAL:
        return PLFS_EINVAL;
    case ENFILE:
        return PLFS_ENFILE;
    case EMFILE:
        return PLFS_EMFILE;
    case ENOTTY:
        return PLFS_ENOTTY;
    case ETXTBSY:
        return PLFS_ETXTBSY;
    case EFBIG:
        return PLFS_EFBIG;
    case ENOSPC:
        return PLFS_ENOSPC;
    case ESPIPE:
        return PLFS_ESPIPE;
    case EROFS:
        return PLFS_EROFS;
    case EMLINK:
        return PLFS_EMLINK;
    case EPIPE:
        return PLFS_EPIPE;
    case ERANGE:
        return PLFS_ERANGE;
    case EDEADLK:
        return PLFS_EDEADLK;
    case ENAMETOOLONG:
        return PLFS_ENAMETOOLONG;
    case ENOSYS:
        return PLFS_ENOSYS;
    case ENOTEMPTY:
        return PLFS_ENOTEMPTY;
    case ELOOP:
        return PLFS_ELOOP;
    case ENODATA:
        return PLFS_ENODATA;
    case EOVERFLOW:
        return PLFS_EOVERFLOW;
    case EPROTOTYPE:
        return PLFS_EPROTOTYPE;
    case ENOTSUP:
        return PLFS_ENOTSUP;
    default:
        return PLFS_TBD;
    }
}

int plfs_error_to_errno(plfs_error_t plfs_err) {
    switch (plfs_err) {
    case PLFS_SUCCESS:
        return 0;
    case PLFS_EPERM:
        return EPERM;
    case PLFS_ENOENT:
        return ENOENT;
    case PLFS_ESRCH:
        return ESRCH;
    case PLFS_EINTR:
        return EINTR;
    case PLFS_EIO:
        return EIO;
    case PLFS_ENXIO:
        return ENXIO;
    case PLFS_E2BIG:
        return E2BIG;
    case PLFS_EBADF:
        return EBADF;
    case PLFS_EAGAIN:
        return EAGAIN;
    case PLFS_ENOMEM:
        return ENOMEM;
    case PLFS_EACCES:
        return EACCES;
    case PLFS_EFAULT:
        return EFAULT;
    case PLFS_EBUSY:
        return EBUSY;
    case PLFS_EEXIST:
        return EEXIST;
    case PLFS_EXDEV:
        return EXDEV;
    case PLFS_ENODEV:
        return ENODEV;
    case PLFS_ENOTDIR:
        return ENOTDIR;
    case PLFS_EISDIR:
        return EISDIR;
    case PLFS_EINVAL:
        return EINVAL;
    case PLFS_ENFILE:
        return ENFILE;
    case PLFS_EMFILE:
        return EMFILE;
    case PLFS_ENOTTY:
        return ENOTTY;
    case PLFS_ETXTBSY:
        return ETXTBSY;
    case PLFS_EFBIG:
        return EFBIG;
    case PLFS_ENOSPC:
        return ENOSPC;
    case PLFS_ESPIPE:
        return ESPIPE;
    case PLFS_EROFS:
        return EROFS;
    case PLFS_EMLINK:
        return EMLINK;
    case PLFS_EPIPE:
        return EPIPE;
    case PLFS_ERANGE:
        return ERANGE;
    case PLFS_EDEADLK:
        return EDEADLK;
    case PLFS_ENAMETOOLONG:
        return ENAMETOOLONG;
    case PLFS_ENOSYS:
        return ENOSYS;
    case PLFS_ENOTEMPTY:
        return ENOTEMPTY;
    case PLFS_ELOOP:
        return ELOOP;
    case PLFS_ENODATA:
        return ENODATA;
    case PLFS_EOVERFLOW:
        return EOVERFLOW;
    case PLFS_EPROTOTYPE:
        return EPROTOTYPE;
    case PLFS_ENOTSUP:
        return ENOTSUP;
    default:
        return 300;
    }
}
const char *
strplfserr(plfs_error_t err) {
    switch (err) {
    case PLFS_SUCCESS:
        return "Success";
    case PLFS_EPERM:
        return "Operation not permitted";
    case PLFS_ENOENT:
        return "No such file or directory";
    case PLFS_ESRCH:
        return "No such process";
    case PLFS_EINTR:
        return "Interrupted system call";
    case PLFS_EIO:
        return "Input/output error";
    case PLFS_ENXIO:
        return "No such device or address";
    case PLFS_E2BIG:
        return "Argument list too long";
    case PLFS_EBADF:
        return "Bad file descriptor";
    case PLFS_EAGAIN:
        return "Resource temporarily unavailable";
    case PLFS_ENOMEM:
        return "Cannot allocate memory";
    case PLFS_EACCES:
        return "Permission denied";
    case PLFS_EFAULT:
        return "Bad address";
    case PLFS_EBUSY:
        return "Device or resource busy";
    case PLFS_EEXIST:
        return "File exists";
    case PLFS_EXDEV:
        return "Invalid cross-device link";
    case PLFS_ENODEV:
        return "No such device";
    case PLFS_ENOTDIR:
        return "Not a directory";
    case PLFS_EISDIR:
        return "Is a directory";
    case PLFS_EINVAL:
        return "Invalid argument";
    case PLFS_ENFILE:
        return "Too many open files in system";
    case PLFS_EMFILE:
        return "Too many open files";
    case PLFS_ENOTTY:
        return "Inappropriate ioctl for device";
    case PLFS_ETXTBSY:
        return "Text file busy";
    case PLFS_EFBIG:
        return "File too large";
    case PLFS_ENOSPC:
        return "No space left on device";
    case PLFS_ESPIPE:
        return "Illegal seek";
    case PLFS_EROFS:
        return "Read-only file system";
    case PLFS_EMLINK:
        return "Too many links";
    case PLFS_EPIPE:
        return "Broken pipe";
    case PLFS_ERANGE:
        return "Numerical result out of range";
    case PLFS_EDEADLK:
        return "Resource deadlock avoided";
    case PLFS_ENAMETOOLONG:
        return "File name too long";
    case PLFS_ENOSYS:
        return "Function not implemented";
    case PLFS_ENOTEMPTY:
        return "Directory not empty";
    case PLFS_ELOOP:
        return "Too many levels of symbolic links";
#if defined (EWOULDBLOCK) && EWOULDBLOCK != EAGAIN
    case PLFS_EWOULDBLOCK:
        return "Operation would block";
#endif
    case PLFS_ENODATA:
        return "No data available";
    case PLFS_EOVERFLOW:
        return "Value too large for defined data type";
    case PLFS_EPROTOTYPE:
        return "Protocol wrong type for socket";
    case PLFS_ENOTSUP:
        return "Not supported";
    default:
        return "Unknown error";
    }
}
