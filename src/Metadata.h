#ifndef __METADATA_H__
#define __METADATA_H__

#include "COPYRIGHT.h"
#include <iostream>
#include <sstream>
using namespace std;

class Metadata {

    public:

    Metadata() { 
        //cerr << "Init metadata " << this << endl;
        synced = true;
        last_offset = total_bytes = reference_count = 0; }

    void addWrite( off_t offset, size_t bytes ) {
        total_bytes += bytes;
        last_offset = ( offset + (off_t)bytes > last_offset 
                            ? offset + bytes : last_offset );
        /*
        ostringstream oss;
        oss << this << " set last_offset " << last_offset 
             << ", set total_bytes " << total_bytes << endl;
        cerr << oss.str();
        */
        synced = false;
    }

    void truncate( off_t offset ) {
        off_t lost_bytes = last_offset - offset;
        last_offset = offset;
        if ( lost_bytes > 0 ) {
            total_bytes -= (size_t)lost_bytes;
        }
            // something is weird here.  make sure we at least get 0 right
        if ( last_offset == 0 ) total_bytes = 0;
    }

    void addMeta( off_t offset, size_t bytes ) {
        last_offset  = max( last_offset, offset ); 
        total_bytes += bytes;
    }

    void getMeta( off_t *offset, size_t *bytes ) {
        *offset = last_offset;
        *bytes  = total_bytes;
        ostringstream oss;
        /*
        oss << this << " last_offset " << last_offset 
             << ", total_bytes " << total_bytes << endl;
        cerr << oss.str();
        */
    }

    int incrementOpens( int amount ) {
        return ( reference_count += amount );
    }

    bool isSynced() {
        return synced;
    }

    bool needsSync() {
        return ! synced;
    }

    void setSynced() {
        synced = true;
    }

    protected:
        off_t  last_offset;
        size_t total_bytes;
        int    reference_count;
        bool   synced;
};

#endif
