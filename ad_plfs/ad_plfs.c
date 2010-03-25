/* -*- Mode: C; c-basic-offset:4 ; -*- */
/* 
 *   $Id: ad_plfs.c,v 1.6 2005/05/23 23:27:44 rross Exp $
 *
 *   Copyright (C) 2001 University of Chicago. 
 *   See COPYRIGHT notice in top-level directory.
 */

#include "ad_plfs.h"

/* adioi.h has the ADIOI_Fns_struct define */
#include "adioi.h"

struct ADIOI_Fns_struct ADIO_PLFS_operations = {
    ADIOI_PLFS_Open, /* Open */
    ADIOI_PLFS_ReadContig, /* ReadContig */
    ADIOI_PLFS_WriteContig, /* WriteContig */
    ADIOI_GEN_ReadStridedColl, /* ReadStridedColl */
    ADIOI_GEN_WriteStridedColl, /* WriteStridedColl */
    ADIOI_GEN_SeekIndividual, /* SeekIndividual */
    ADIOI_PLFS_Fcntl, /* Fcntl */
    ADIOI_GEN_SetInfo, /* SetInfo */
    ADIOI_GEN_ReadStrided, /* ReadStrided */
    ADIOI_GEN_WriteStrided, /* WriteStrided */
    ADIOI_PLFS_Close, /* Close */
    ADIOI_FAKE_IreadContig, /* IreadContig */
    ADIOI_FAKE_IwriteContig, /* IwriteContig */
    ADIOI_FAKE_IODone, /* ReadDone */
    ADIOI_FAKE_IODone, /* WriteDone */
    ADIOI_FAKE_IOComplete, /* ReadComplete */
    ADIOI_FAKE_IOComplete, /* WriteComplete */
    ADIOI_FAKE_IreadStrided, /* IreadStrided */
    ADIOI_FAKE_IwriteStrided, /* IwriteStrided */
    ADIOI_PLFS_Flush, /* Flush */
    ADIOI_PLFS_Resize, /* Resize */
    ADIOI_PLFS_Delete, /* Delete */
};

int ad_plfs_amode( int access_mode ) {
    int amode = 0; // O_META;
    if (access_mode & ADIO_RDONLY)
        amode = amode | O_RDONLY;
    if (access_mode & ADIO_WRONLY)
        amode = amode | O_WRONLY;
    if (access_mode & ADIO_RDWR)
        amode = amode | O_RDWR;
    if (access_mode & ADIO_EXCL)
        amode = amode | O_EXCL;
    return amode;
}
