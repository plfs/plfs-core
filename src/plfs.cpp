#include "plfs.h"
#include "plfs_private.h"
#include "Index.h"
#include "WriteFile.h"
#include "Container.h"
#include "Util.h"
#include "OpenFile.h"

#include <stdarg.h>
#include <limits>
#include <assert.h>

// a shortcut for functions that are expecting zero
int 
retValue( int res ) {
    return Util::retValue(res);
}

int 
plfs_create( const char *path, mode_t mode, int flags ) {
    int attempts = 0;
    return Container::create( path, Util::hostname(), mode, flags, &attempts );
}

int
addWriter( WriteFile *wf, pid_t pid, const char *path, mode_t mode ) {
    int ret = -ENOENT;
    for( int attempts = 0; attempts < 2 && ret == -ENOENT; attempts++ ) {
        ret = wf->addWriter( pid ); 
        if ( ret == -ENOENT ) {
            // maybe the hostdir doesn't exist
            Container::makeHostDir( path, Util::hostname(), mode );
        }
    }
    return ret;
}
int
isWriter( int flags ) {
    return (flags & O_WRONLY || flags & O_RDWR);
}

// this requires that the supplementary groups for the user are set
int 
plfs_chown( const char *path, uid_t u, gid_t g ) {
    return Container::Chown( path, u, g );
}

int
is_plfs_file( const char *path ) {
    return Container::isContainer( path );
}

void 
plfs_debug( FILE *fp, const char *format, ... ) {
    va_list args;
    va_start(args, format);
    Util::Debug(fp, format, args);
    va_end( args );
}

int
plfs_access( const char *path, int mask ) {
    int ret = -1;
    if ( Container::isContainer( path ) ) {
        ret = retValue( Container::Access( path, mask ) );
    } else {
        ret = retValue( Util::Access( path, mask ) );
    }
    return ret;
}
int 
plfs_chmod( const char *path, mode_t mode ) {
    int ret = -1;
    if ( Container::isContainer( path ) ) {
        ret = retValue( Container::Chmod( path, mode ) );
    } else {
        ret = retValue( Util::Chmod( path, mode ) );
    }
    return ret;
}


int
plfs_utime( const char *path, struct utimbuf *ut ) {
    int ret = -1;
    if ( Container::isContainer( path ) ) {
        ret = Container::Utime( path, ut );
    } else {
        ret = retValue( Util::Utime( path, ut ) );
    }
    return ret;
}

// returns bytes read or -errno
ssize_t 
read_helper( Index *index, char *buf, size_t size, off_t offset ) {
    off_t  chunk_offset = 0;
    size_t chunk_length = 0;
    int    fd           = -1;
    int ret;

    // need to lock this since the globalLookup does the open of the fds
    ret = index->globalLookup( &fd, &chunk_offset, &chunk_length, offset );
    if ( ret != 0 ) return ret;

    ssize_t read_size = ( size < chunk_length ? size : chunk_length );
    if ( read_size > 0 ) {
        // uses pread to allow the fd's to be shared 
        if ( fd >= 0 ) {
            ret = Util::Pread( fd, buf, read_size, chunk_offset );
            if ( ret < 0 ) {
                cerr << "Couldn't read in " << fd << ":" 
                     << strerror(errno) << endl;
                return -errno;
            }
        } else {
            // zero fill the hole
            memset( (void*)buf, 0, read_size);
            ret = read_size;
        }
    } else {
        // when chunk_length = 0, that means we're at EOF
        ret = 0;
    }
    return ret;
}

// returns -errno or bytes read
ssize_t 
plfs_read( Plfs_fd *pfd, char *buf, size_t size, off_t offset ) {
	int ret = 0;
    Index *index = pfd->getIndex(); 
    bool new_index_created = false;  
    const char *path = pfd->getPath();

    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);

    // possible that we opened the file as O_RDWR
    // if so, build an index now, but destroy it after this IO
    // so that new writes are re-indexed for new reads
    // basically O_RDWR is possible but it can reduce read BW

    if ( index == NULL ) {
        index = new Index( path );
        if ( index ) {
            new_index_created = true;
            ret = Container::populateIndex( path, index );
        } else {
            ret = -EIO;
        }
    }

    // now that we have an index (newly created or otherwise), go ahead and read
    if ( ret == 0 ) {
            // if the read spans multiple chunks, we really need to loop in here
            // to get them all read bec the man page for read says that it 
            // guarantees to return the number of bytes requested up to EOF
            // so any partial read by the client indicates EOF and a client
            // may not retry
        ssize_t bytes_remaining = size;
        ssize_t bytes_read      = 0;
        do {
            ret = read_helper( index, &(buf[bytes_read]), bytes_remaining, 
                    offset + bytes_read );
            if ( ret > 0 ) {
                bytes_read      += ret;
                bytes_remaining -= ret;
            }
        } while( bytes_remaining && ret > 0 );
        ret = ( ret < 0 ? ret : bytes_read );
    }

    if ( new_index_created ) {
        Util::Debug( stderr, "%s removing freshly created index for %s\n",
                __FUNCTION__, path );
        delete( index );
        index = NULL;
    }
    return ret;
}

int
plfs_rename( Plfs_fd *pfd, const char *from, const char *to ) {
    int ret = retValue( Util::Rename( from, to ) );
    if ( ret == 0 && pfd ) {
        pfd->setPath( to );
    }
    return ret;
}

// pass in a NULL Plfs_fd to have one created for you
// pass in a valid one to add more writers to it
// one problem is that we fail if we're asked to overwrite a normal file
int
plfs_open( Plfs_fd **pfd, const char *path, int flags, pid_t pid, mode_t mode )
{
    WriteFile *wf      = NULL;
    Index     *index   = NULL;
    int ret            = 0;
    bool new_writefile = false;
    bool new_index     = false;
    bool new_pfd       = false;

    // ugh, no idea why this line is here or what it does 
    if ( mode == 420 || mode == 416 ) mode = 33152; 

    // make sure we're allowed to open this container
    // this breaks things when tar is trying to create new files
    // with --r--r--r bec we create it w/ that access and then 
    // we can't write to it
    //ret = checkAccess( strPath, fi ); 

    if ( flags & O_CREAT ) {
        ret = plfs_create( path, mode, flags ); 
    }

    if ( flags & O_TRUNC ) {
        ret = plfs_trunc( NULL, path, 0 );
    }

    if ( *pfd) plfs_reference_count(*pfd);

    // this next chunk of code works similarly for writes and reads
    // for writes, create a writefile if needed, otherwise add a new writer
    // create the write index file after the write data file so that the
    // hostdir is created
    // for reads, create an index if needed, otherwise add a new reader
    if ( ret == 0 && isWriter(flags) ) {
        if ( *pfd ) {
            wf = (*pfd)->getWritefile();
        } 
        if ( wf == NULL ) {
            wf = new WriteFile( path, Util::hostname(), mode ); 
            new_writefile = true;
        }
        if ( ret == 0 ) {
            ret = addWriter( wf, pid, path, mode );
            Util::Debug( stderr, "%s added writer: %d\n", __FUNCTION__, ret );
            if ( ret > 0 ) ret = 0; // add writer returns # of current writers
            if ( ret == 0 && new_writefile ) ret = wf->openIndex( pid ); 
        }
        if ( ret != 0 && wf ) {
            delete wf;
            wf = NULL;
        }
    } else if ( ret == 0 ) {
        if ( *pfd ) {
            index = (*pfd)->getIndex();
        }
        if ( index == NULL ) {
            index = new Index( path );  
            new_index = true;
            ret = Container::populateIndex( path, index );
        }
        if ( ret == 0 ) {
            index->incrementOpens(1);
        }
        if ( ret != 0 && index ) {
            delete index;
            index = NULL;
        }
    }

    if ( ret == 0 && ! *pfd ) {
        *pfd = new Plfs_fd( wf, index, pid, mode, path ); 
        new_pfd       = true;
        // we create one open record for all the pids using a file
        // only create the open record for files opened for writing
        if ( wf ) {
            ret = Container::addOpenrecord( path, Util::hostname(), pid );
        }
        //cerr << __FUNCTION__ << " added open record for " << path << endl;
    } else if ( ret == 0 ) {
        if ( wf && new_writefile) (*pfd)->setWritefile( wf ); 
        if ( index && new_index ) (*pfd)->setIndex( index  ); 
    }
    if ( ret == 0 ) (*pfd)->incrementOpens(1);
    plfs_reference_count(*pfd);
    return ret;
}

ssize_t 
plfs_write( Plfs_fd *pfd, const char *buf, size_t size, off_t offset, pid_t pid)
{

    // this can fail because this call is not in a mutex so it's possible
    // that some other thread in a close is changing ref counts right now
    // but it's OK that the reference count is off here since the only
    // way that it could be off is if someone else removes their handle,
    // but no-one can remove the handle being used here except this thread
    // which can't remove it now since it's using it now
    //plfs_reference_count(pfd);

    int ret = 0; ssize_t written;
    WriteFile *wf = pfd->getWritefile();

    Util::Debug( stderr, "Write to %s, offset %ld\n", 
            pfd->getPath(), (long)offset );
    ret = written = wf->write( buf, size, offset, pid );

    return ( ret >= 0 ? written : ret );
}

int 
plfs_sync( Plfs_fd *pfd, pid_t pid ) {
    return ( pfd->getWritefile() ? pfd->getWritefile()->sync(pid) : 0 );
}

// this can fail due to silly rename 
// imagine an N-1 normal file on PanFS that someone is reading and
// someone else is unlink'ing.  The unlink will see a reference count
// and will rename it to .panfs.blah.  The read continues and when the
// read releases the reference count on .panfs.blah drops to zero and
// .panfs.blah is unlinked
// but in our containers, here's what happens:  a chunk or an index is
// open by someone and is unlinked by someone else, the silly rename 
// does the same thing and now the container has a .panfs.blah file in
// it.  Then when we try to remove the directory, we get a ENOTEMPTY.
// truncate only removes the droppings but leaves the directory structure
// 
// we also use this function to implement truncate on a container
// in that case, we remove all droppings but preserve the container
// an empty container = empty file
//
// this code should really be moved to container
int 
removeDirectoryTree( const char *path, bool truncate_only ) {
    DIR *dir;
    struct dirent *ent;
    int ret = 0;
    Util::Debug( stderr, "%s on %s\n", __FUNCTION__, path );

    dir = opendir( path );
    if ( dir == NULL ) return -errno;

    while( (ent = readdir( dir ) ) != NULL ) {
        if ( ! strcmp(ent->d_name, ".") || ! strcmp(ent->d_name, "..") ) {
            //Util::Debug( stderr, "skipping %s\n", ent->d_name );
            continue;
        }
        if ( ! strcmp(ent->d_name, ACCESSFILE ) && truncate_only ) {
            continue;   // don't remove our accessfile!
        }
        if ( ! strcmp( ent->d_name, OPENHOSTDIR ) && truncate_only ) {
            continue;   // don't remove open hosts
        }
        string child( path );
        child += "/";
        child += ent->d_name;
        if ( Util::isDirectory( child.c_str() ) ) {
            if ( removeDirectoryTree( child.c_str(), truncate_only ) != 0 ) {
                ret = -errno;
                continue;
            }
        } else {
            //Util::Debug( stderr, "unlinking %s\n", ent->d_name );
            // ok, we seem to be screwing things up by deleting a handle
            // that someone else has open:
            // e.g. two writers do an open with O_TRUNC at the same time
            // one writer finishes the O_TRUNC and then opens it's fd's
            // then the next writer comes and does the O_TRUNC and removes
            // the first writer's open files.  Instead let's just truncate
            // each open file
            int remove_ret = 0;
            if ( truncate_only ) remove_ret = Util::Truncate(child.c_str(), 0);
            else                 remove_ret = Util::Unlink  (child.c_str());
            
            // ENOENT would be OK, could mean that an openhosts dropping
            // was removed on another host or something
            if ( remove_ret != 0 && errno != ENOENT ) {
                ret = -errno;
                continue;
            }
        }
    }
    if ( closedir( dir ) != 0 ) {
        ret = -errno;
    }

    // here we have deleted all children.  Now delete the directory itself
    // if truncate is called, this means it's a container.  so we only
    // delete the internal stuff but not the directory structure
    if ( ret != 0 ) return ret;
    if ( truncate_only ) {
        return 0;
    } else {
        ret = Util::Rmdir( path );
        if ( ret != 0 && errno == ENOTEMPTY ) {
            int prec = numeric_limits<long double>::digits10; // 18
            ostringstream trash_path;
            trash_path.precision(prec); // override the default of 6
            trash_path << "." << path << "." << Util::hostname() << "." 
                       << Util::getTime() << ".silly_rename";
            cerr << "Need to silly rename " << path << " to "
                 << trash_path.str().c_str() << endl;
            Util::Rename( path, trash_path.str().c_str() ); 
            ret = 0;
        }
        return retValue( ret );
    }
}
            
// this should only be called if the uid has already been checked
// and is allowed to access this file
// Plfs_fd can be NULL
// returns 0 or -errno
int 
plfs_getattr( Plfs_fd *of, const char *path, struct stat *stbuf ) {
    int ret = 0;
    if ( path == NULL && of ) path = of->getPath();
    if ( ! Container::isContainer( path ) ) {
        ret = retValue( Util::Lstat( path, stbuf ) );
    } else {
        ret = Container::getattr( path, stbuf );
        if ( ret == 0 ) {
            // is it also open currently?
            // we might be reading from some other users writeFile but
            // we're only here if we had access to stat the container
            // commented out some stuff that I thought was maybe slow
            // this might not be necessary depending on how we did 
            // the stat.  If we stat'ed data droppings then we don't
            // need to do this but it won't hurt.  
            // If we were trying to read index droppings
            // and they weren't available, then we should do this.
            WriteFile *wf=(of && of->getWritefile() ? of->getWritefile() :NULL);
            if ( wf ) {
                off_t  last_offset;
                size_t total_bytes;
                wf->getMeta( &last_offset, &total_bytes );
                if ( last_offset > stbuf->st_size ) {
                    stbuf->st_size = last_offset;
                }
                    // if the index has already been flushed, then we might
                    // count it twice here.....
                stbuf->st_blocks += Container::bytesToBlocks(total_bytes);
            }
        }
    }
    //cerr << __FUNCTION__ << " of " << path << "(" 
    //     << (of == NULL ? "closed" : "open") 
    //     << ") size is " << stbuf->st_size << endl;

    return ret;
}

// this is called when truncate has been used to extend a file
// returns 0 or -errno
int 
extendFile( Plfs_fd *of, string strPath, off_t offset ) {
    int ret = 0;
    bool newly_opened = false;
    WriteFile *wf = ( of && of->getWritefile() ? of->getWritefile() : NULL );
    pid_t pid = ( of ? of->getPid() : 0 );
    if ( wf == NULL ) {
        mode_t mode = Container::getmode( strPath.c_str() ); 
        wf = new WriteFile( strPath.c_str(), Util::hostname(), mode );
        ret = wf->openIndex( pid );
        newly_opened = true;
    }
    if ( ret == 0 ) ret = wf->extend( offset );
    if ( newly_opened ) {
        delete wf;
        wf = NULL;
    }
    return ret;
}

// the Plfs_fd can be NULL
int 
plfs_trunc( Plfs_fd *of, const char *path, off_t offset ) {
    if ( ! Container::isContainer( path ) ) {
        // this is weird, we expect only to operate on containers
        return retValue( truncate(path,offset) );
    }

    int ret = 0;
    if ( offset == 0 ) {
            // this is easy, just remove all droppings
        ret = removeDirectoryTree( path, true );
    } else {
            // either at existing end, before it, or after it
        struct stat stbuf;
        ret = plfs_getattr( of, path, &stbuf );
        if ( ret == 0 ) {
            if ( stbuf.st_size == offset ) {
                ret = 0; // nothing to do
            } else if ( stbuf.st_size > offset ) {
                ret = Container::Truncate( path, offset ); // make smaller
            } else if ( stbuf.st_size < offset ) {
                ret = extendFile( of, path, offset );    // make bigger
            }
        }
    }

    // if we actually modified the container, update any open file handle
    if ( ret == 0 && of && of->getWritefile() ) {
        ret = of->getWritefile()->truncate( offset );
        of->truncate( offset );
            // here's a problem, if the file is open for writing, we've
            // already opened fds in there.  So the droppings are
            // deleted/resized and our open handles are messed up 
            // it's just a little scary if this ever happens following
            // a rename because the writefile will attempt to restore
            // them at the old path....
        if ( ret == 0 && of && of->getWritefile() ) {
            ret = of->getWritefile()->restoreFds();
        }
    }

    return ret;
}

int 
plfs_unlink( const char *path ) {
    int ret = 0;
    if ( Container::isContainer( path ) ) {
        ret = removeDirectoryTree( path, false );  
    } else {
        ret = retValue( unlink( path ) );   // recurse
    }
    return ret; 
}

int
plfs_query( Plfs_fd *pfd, size_t *writers, size_t *readers ) {
    WriteFile *wf = pfd->getWritefile();
    Index     *ix = pfd->getIndex();
    *writers = 0;   *readers = 0;

    if ( wf ) {
        *writers = wf->numWriters();
    }

    if ( ix ) {
        *readers = ix->incrementOpens(0);
    }
    return 0;
}

ssize_t plfs_reference_count( Plfs_fd *pfd ) {
    WriteFile *wf = pfd->getWritefile();
    Index     *in = pfd->getIndex();
    
    int ref_count = 0;
    if ( wf ) ref_count += wf->numWriters();
    if ( in ) ref_count += in->incrementOpens(0);
    if ( ref_count != pfd->incrementOpens(0) ) {
        ostringstream oss;
        oss << __FUNCTION__ << " not equal counts: " << ref_count
            << " != " << pfd->incrementOpens(0) << endl;
        Util::Debug( stderr, "%s", oss.str().c_str() ); 
        assert( 0 );
    }
    return ref_count;
}

// returns number of open handles or -errno
int
plfs_close( Plfs_fd *pfd, pid_t pid, int open_flags ) {
    int ret = 0;
    WriteFile *wf    = pfd->getWritefile();
    Index     *index = pfd->getIndex();
    size_t writers = 0, readers = 0, ref_count = 0;

    // be careful.  We might enter here when we have both writers and readers
    // make sure to remove the appropriate open handle for this thread by 
    // using the original open_flags

    // clean up after writes
    if ( isWriter(open_flags) ) {
        assert(wf);
        writers = wf->removeWriter( pid );
        if ( writers == 0 ) {
            off_t  last_offset;
            size_t total_bytes;
            wf->getMeta( &last_offset, &total_bytes );
            Container::addMeta( last_offset, total_bytes, pfd->getPath(), 
                    Util::hostname() );
            // the pfd remembers the first pid added which happens to be the
            // one we used to create the open-record
            Container::removeOpenrecord( pfd->getPath(), Util::hostname(), 
                    pfd->getPid() );
            delete wf;
            wf = NULL;
            pfd->setWritefile(NULL);
        } else if ( writers < 0 ) {
            ret = writers;
            writers = wf->numWriters();
        } else if ( writers > 0 ) {
            ret = 0;
        }
    } else {
        assert( index );
        readers = index->incrementOpens(-1);
        if ( readers == 0 ) {
            delete index;
            index = NULL;
            pfd->setIndex(NULL);
        }
    }

    ref_count = pfd->incrementOpens(-1);
    Util::Debug( stderr, "%s %s: %d readers, %d writers, %d refs remaining\n",
            __FUNCTION__, pfd->getPath(), (int)readers, (int)writers,
            (int)ref_count);

    // make sure the reference counting is correct 
    plfs_reference_count(pfd);    
    if ( ret == 0 && ref_count == 0 ) {
        ostringstream oss;
        oss << __FUNCTION__ << " removing OpenFile " << pfd << endl;
        Util::Debug( stderr, "%s", oss.str().c_str() ); 
        delete pfd; 
        pfd = NULL;
    }

    return ( ret < 0 ? ret : ref_count );
}
