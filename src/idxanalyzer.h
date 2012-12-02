#ifndef __idxanalyzer_h__
#define __idxanalyzer_h__

#include <vector>
#include <map>
#include <string>
#include <iostream>
#include <fstream>
#include <assert.h>
#include <unistd.h>
#include <stdio.h>
#include <sstream>
#include <stdint.h>
#include "mlogfacs.h"

using namespace std;

template <class T> class SigStack;
template <class T> class PatternStack;
class IdxSigEntryList;
class HostEntry;
class ContainerIdxSigEntry;
// this namespace is not used anymore
namespace MultiLevel {
    class PatternCombo;
    class ChunkMap;
}

typedef int32_t header_t; //the type to hold the body size in serialization

void appendToBuffer( string &to, const void *from, const int size );

//Note that this function will increase start
void readFromBuf( string &from, void *to, int &start, const int size );

bool isContain( off_t off, off_t offset, off_t length );
off_t sumVector( vector<off_t> seq );


//used to describe a single pattern that is found
//This will be saved in the stack.
//For example, (seq)^cnt=(3,5,8)^2
class PatternUnit {
    public:
        vector<off_t> seq;
        int64_t cnt; //count of repeatition
        
        PatternUnit() {}
        PatternUnit( vector<off_t> sq, int ct )
            :seq(sq),cnt(ct)
        {}
        
        int size() const;
        virtual string show() const;
};

//This is used to describ a single repeating
//pattern, but with starting value
//Sig is short for Signature(pattern).
class IdxSigUnit: public PatternUnit {
    public:
        off_t init; // the initial value of 
                    // logical offset, length, or physical offset
        string show() const;

        header_t bodySize();
        string serialize();
        void deSerialize(string buf);
        off_t getValByPos( const int &pos ) ;
        bool append( IdxSigUnit &other );
        bool isSeqRepeating();
        void compressRepeats();
};

template <class T> // T can be PatternUnit or IdxSigUnit
class PatternStack {
    public:
        PatternStack() {}
        void push( T pu ); 
        void clear() ;
        bool isPopSafe( int t ); 
        bool popElem ( int t );
        //pop out one pattern
        void popPattern ();
        
        //make sure the stack is not empty before using this
        T top ();
        typename vector<T>::const_iterator begin() const;
        typename vector<T>::const_iterator end() const;
        virtual string show();
        int bodySize();
        string serialize();    
        void deSerialize( string buf );
        
        vector<T> the_stack;
};

//I really should not call it a stack since I use
//it in many ways...
template <class T>
class SigStack: public PatternStack <T> 
{
    public:
        virtual string show()
        {
            typename vector<T>::const_iterator iter;
            ostringstream showstr;
            for ( iter = this->the_stack.begin();
                    iter != this->the_stack.end();
                    iter++ )
            {
                vector<off_t>::const_iterator off_iter;
                showstr << iter->init << "- (" ;
                for ( off_iter = (iter->seq).begin();
                        off_iter != (iter->seq).end();
                        off_iter++ )
                {
                    showstr << *off_iter << ", ";
                }
                showstr << ")^" << iter->cnt << endl;
            }
            return showstr.str(); 
        }
        
        int size()
        {
            typename vector<T>::const_iterator iter;
            int size = 0;
            for ( iter = this->the_stack.begin();
                    iter != this->the_stack.end();
                    iter++ )
            {
                size += iter->size();
            }
            return size;
        }

        off_t getValByPos( const int &pos );
};


// This Tuple is a concept in the LZ77 compression algorithm.
// Please refer to http://jens.quicknote.de/comp/LZ77-JensMueller.pdf for details.
class Tuple {
    public:
        int offset; //note that this is not the 
        // offset when accessing file. But
        // the offset in LZ77 algorithm
        int length; //concept in LZ77
        off_t next_symbol;

        Tuple() {}
        Tuple(int o, int l, off_t n) {
            offset = o;
            length = l;
            next_symbol = n;
        }

        void put(int o, int l, off_t n) {
            offset = o;
            length = l;
            next_symbol = n;
        }

        bool operator== (const Tuple other) {
            if (offset == other.offset 
                    && length == other.length
                    && next_symbol == other.next_symbol)
            {
                return true;
            } else {
                return false;
            }
        }

        // Tell if the repeating sequences are next to each other
        bool isRepeatingNeighbor() {
            return (offset == length && offset > 0);
        }

        string show() {
            ostringstream showstr;
            showstr << "(" << offset 
                << ", " << length
                << ", " << next_symbol << ")" << endl;
            return showstr.str();
        }
};

/* Not longer in use
 *
class IdxEntry {
    public:
        int Proc;
#define ID_WRITE 0
#define ID_READ  1
        int ID; //either ID_WRITE or ID_READ
        off_t Logical_offset;
        off_t Length;
        double Begin_timestamp;
        double End_timestamp;
        off_t Logical_tail;
        int ID_2;
        off_t Physical_offset;
};
*/

// This is actually the old HostEntry. I moved it here for convenience.
// Not a good practice though.
// TODO:
// Move this class somewhere proper. It is impelmented in Index.cpp
class HostEntry
{
    public:
        HostEntry();
        HostEntry( off_t o, size_t s, pid_t p );
        HostEntry( const HostEntry& copy );
        bool overlap( const HostEntry& );
        bool contains ( off_t ) const;
        bool splittable ( off_t ) const;
        bool abut   ( const HostEntry& );
        off_t logical_tail( ) const;
        bool follows(const HostEntry&);
        bool preceeds(const HostEntry&);

    protected:
        off_t  logical_offset;
        off_t  physical_offset;  // I tried so hard to not put this in here
        // to save some bytes in the index entries
        // on disk.  But truncate breaks it all.
        // we assume that each write makes one entry
        // in the data file and one entry in the index
        // file.  But when we remove entries and
        // rewrite the index, then we break this
        // assumption.  blech.
        size_t length;
        double begin_timestamp;
        double end_timestamp;
        pid_t  id;      // needs to be last so no padding

        friend class Index;
        friend class IdxSignature;
        friend class IdxSigEntryList;
        friend class MultiLevel::PatternCombo;
};

// Each index has its own signature
// This is the utility used to recognize patterns.
class IdxSignature {
    public:
        IdxSignature():win_size(6) {} 
        void discoverPattern( vector<off_t> const &seq );
        SigStack<IdxSigUnit> discoverSigPattern( vector<off_t> const &seq,
                vector<off_t> const &orig );
        //It takes in a entry buffer like in PLFS,
        //analyzes it and generate Index Signature Entries
        IdxSigEntryList generateIdxSignature(vector<HostEntry> &entry_buf, int proc);
        SigStack<IdxSigUnit> findPattern( vector<off_t> deltas );
        SigStack<IdxSigUnit> generateComplexPatterns( vector<off_t> inits );
    private:
        int win_size; //window size
        Tuple searchNeighbor( vector<off_t> const &seq,
                vector<off_t>::const_iterator p_lookahead_win ); 
};

// This is the pattern entry.
class IdxSigEntry {
    public:
        pid_t original_chunk;  //used only when entry is in global
                               //complex index. 
        pid_t new_chunk_id;    //This is not serialized yet.
                               //it should only be serialized in
                               //the context of global complex index
                               //Update: Now serialized
        double begin_timestamp; //it has not been used yet. 
        double end_timestamp;
        IdxSigUnit logical_offset;
        SigStack<IdxSigUnit> length;
        SigStack<IdxSigUnit> physical_offset;

        string serialize();
        void deSerialize(string buf);
        int bodySize();
        bool contains( const off_t &offset, int &pos );
        string show();
        bool append(IdxSigEntry &other);
        friend ostream& operator <<(ostream&, IdxSigEntry&);
};

class SigChunkMap {
    public:
        pid_t original_chunk_id;
        pid_t new_chunk_id;
        pid_t physical_bulk_id;
};

// This is a pattern entry in meory. 
class ContainerIdxSigEntry {
    public:
        double begin_timestamp;
        double end_timestamp;
        vector<SigChunkMap> chunkmap;
        IdxSigUnit logical_offset;
        SigStack<IdxSigUnit> length;
        SigStack<IdxSigUnit> physical_offset;
        bool  preprocessed;
        off_t len;
        off_t stride;
        int   num_chunks_per_seg;
        off_t bulk_size;
        int   num_of_bulks;
        int   last_bulk_chunks;     
        
        ContainerIdxSigEntry();
        void preprocess();
        string serialize();        
        void deSerialize(string buf);
        int bodySize();
        bool contains( const off_t &req_offset,
                                     off_t &o_offset,
                                     off_t &o_length,
                                     off_t &o_physical,
                                     off_t &o_new_chunk_id);
        string show() const;
        //bool append(IdxSigEntry &other);
        friend ostream& operator <<(ostream&, ContainerIdxSigEntry&);
};


// This is a list of pattern entries in memory for lookups.
class ContainerIdxSigEntryList {
    public:
        map<off_t, ContainerIdxSigEntry> listmap;
        map<off_t, ContainerIdxSigEntry>::iterator last_hit;

        ContainerIdxSigEntryList();
        void insertGlobal(const ContainerIdxSigEntry &entry);
        void insertEntry(const ContainerIdxSigEntry &entry);
        string show() const;
        string serialize();
        void deSerialize(string buf);
        int bodySize();
        void crossProcMerge();
        bool lookup( const off_t &req_offset,
                                     off_t &o_offset,
                                     off_t &o_length,
                                     off_t &o_physical,
                                     off_t &o_new_chunk_id);
};


class IdxSigEntryList {
    public:
        vector<IdxSigEntry> list; // the pattern list
        vector<HostEntry> messies; // this traditional one-to-one entries.

    public:
        void append(IdxSigEntryList other);
        void append(IdxSigEntry other, bool compress=false);
        void append(vector<IdxSigEntry> &other);
        string show() ;
        void saveToFile(const int fd);
        void saveMessiesToFile(const int fd);
        void saveListToFile(const int fd);
        void clear();
        string serialize();
        string & serializeMessies( string &buf );
        void deSerializeMessies( string &buf );
        void deSerialize(string buf);
        int bodySize();
        void dumpMessies();
        void messiesToPatterns();

};

string printIdxEntries( vector<IdxSigEntry> &idx_entry_list );
vector<off_t> buildDeltas( vector<off_t> seq );

// Below are some function bodies that we have to put here since
// template is used.


// Serilize a PatternStack to a binary stream
template <class T>
string 
PatternStack<T>::serialize()
{
    header_t bodysize = 0;
    string buf;
    typename vector<T>::iterator iter;
    header_t realtotalsize = 0;

    bodysize = bodySize();
    //cout << "data size put in: " << bodysize << endl;
    appendToBuffer(buf, &bodysize, sizeof(bodysize));
    for ( iter = the_stack.begin() ; 
            iter != the_stack.end() ;
            iter++ )
    {
        string unit = iter->serialize();
        appendToBuffer(buf, &unit[0], unit.size());
        realtotalsize += unit.size();
        //to test if it serilized right
        //IdxSigUnit tmp;
        //tmp.deSerialize(unit);
        //cout << "test show.\n";
        //tmp.show();
    }
    assert(realtotalsize == bodysize);
    return buf;
}

template <class T>
void
PatternStack<T>::deSerialize( string buf )
{
    header_t bodysize, bufsize;
    int cur_start = 0;
    
    clear(); 

    readFromBuf(buf, &bodysize, cur_start, sizeof(bodysize));
    
    bufsize = buf.size();
    assert(bufsize == bodysize + sizeof(bodysize));
    while ( cur_start < bodysize ) {
        header_t unitbodysize;
        string unitbuf;
        T sigunit;

        readFromBuf(buf, &unitbodysize, cur_start, sizeof(unitbodysize));
        //cout << "Unitbodysize:" << unitbodysize << endl;
        int sizeofheadandunit = sizeof(unitbodysize) + unitbodysize;
        unitbuf.resize(sizeofheadandunit);
        if ( unitbodysize > 0 ) {
            //TODO:make this more delegate
            cur_start -= sizeof(unitbodysize);
            readFromBuf(buf, &unitbuf[0], cur_start, sizeofheadandunit); 
        }
        sigunit.deSerialize(unitbuf);
        push(sigunit);
    }

}


template <class T>
int
PatternStack<T>::bodySize()
{
    int totalsize = 0;
    typename vector<T>::iterator iter;
    for ( iter = the_stack.begin() ; 
            iter != the_stack.end() ;
            iter++ )
    {
        //IdxSigUnit body size and its header
        totalsize += (iter->bodySize() + sizeof(header_t));
    }
    return totalsize;
}

template <class T>
void PatternStack<T>::push( T pu ) 
{
    the_stack.push_back(pu);
}

template <class T>
void PatternStack<T>::clear() 
{
    the_stack.clear();
}

// if popping out t elements breaks any patterns?
template <class T>
bool PatternStack<T>::isPopSafe( int t ) 
{
    typename vector<T>::reverse_iterator rit;
    
    int total = 0;
    rit = the_stack.rbegin();
    while ( rit != the_stack.rend()
            && total < t )
    {
        total += rit->size();
        rit++;
    }
    return total == t;
}

// return false if it is not safe
// t is number of elements, not pattern unit
template <class T>
bool PatternStack<T>::popElem ( int t )
{
    if ( !isPopSafe(t) ) {
        return false;
    }

    int total = 0; // the number of elem already popped out
    while ( !the_stack.empty() && total < t ) {
        total += top().size();
        the_stack.pop_back();
    }
    assert( total == t );

    return true;
}

//pop out one pattern
template <class T>
void PatternStack<T>::popPattern () 
{
    the_stack.pop_back();
}

//make sure the stack is not empty before using this
template <class T>
T PatternStack<T>::top () 
{
    assert( the_stack.size() > 0 );
    return the_stack.back();
}

template <class T>
typename vector<T>::const_iterator
PatternStack<T>::begin() const
{
    return the_stack.begin();
}

template <class T>
typename vector<T>::const_iterator
PatternStack<T>::end() const
{
    return the_stack.end();
}

template <class T>
string 
PatternStack<T>::show()
{
    typename vector<T>::const_iterator iter;
    ostringstream showstr;

    for ( iter = the_stack.begin();
            iter != the_stack.end();
            iter++ )
    {
        showstr << iter->show();
        /*
        vector<off_t>::const_iterator off_iter;
        for ( off_iter = (iter->seq).begin();
                off_iter != (iter->seq).end();
                off_iter++ )
        {
            cout << *off_iter << ", ";
        }
        cout << "^" << iter->cnt << endl;
        */
    }
    return showstr.str();
}

// Get the pos th element in the pattern
// pos has to be in the range
template <class T>
inline
off_t SigStack<T>::getValByPos( const int &pos ) 
{
    int cur_pos = 0; //it should always point to sigunit.init

    typename vector<T>::iterator iter;
	
    for ( iter = this->the_stack.begin() ;
          iter != this->the_stack.end() ;
          iter++ )
    {
        int numoflen = iter->seq.size() * iter->cnt;
        
        if ( numoflen == 0 ) {
            numoflen = 1; //there's actually one element in *iter
        } 
        if (cur_pos <= pos && pos < cur_pos + numoflen ) {
            //in the range that pointed to by iter
            int rpos = pos - cur_pos;
            return iter->getValByPos( rpos );
        } else {
            //not in the range of iter
            cur_pos += numoflen; //keep track of current pos
        }
    }
    assert(0); // Make it hard for the errors
}


// Decide whether offset is in this IdxSigEntry
// Let assume there's no overwrite TODO:  this
inline
bool IdxSigEntry::contains( const off_t &offset, int &pos )
{
    //mlog(IDX_WARN, "EEEntering %s", __FUNCTION__);
    //ostringstream oss;
    //oss << show() << "LOOKING FOR:" << offset << endl;
    //mlog(IDX_WARN, "%s", oss.str().c_str());

    vector<IdxSigUnit>::const_iterator iter;
    vector<off_t>::const_iterator iiter;
    vector<off_t>::const_iterator off_delta_iter;
    const off_t &logical = offset;

    off_t delta_sum;
        
    delta_sum = sumVector(logical_offset.seq);
    
    //ostringstream oss;
    //oss << delta_sum;
    //mlog(IDX_WARN, "delta_sum:%s", oss.str().c_str());

    // At this time, let me do it in the stupidest way
    // It works for all cases. Not bad.
    /*
    int size = logical_offset.seq.size() * logical_offset.cnt;
    int i;
    for ( i = 0 ; 
          i < size 
          || i == 0 ; //have to check the first one.
          i++ ) {
        //ostringstream oss;
        //oss << "i:" << i << "size:" << size << endl;
        //mlog(IDX_WARN, "%s", oss.str().c_str());
        if ( isContain(offset, logical_offset.getValByPos(i),
                               length.getValByPos(i) ) )
        {
            pos = i;
            return true;
        }
    }
    return false;
    */

    ///////////////////////////////////////////////////
    if ( offset < logical_offset.init ) {
        //mlog(IDX_WARN, "offset < init");
        return false;
    }

    if (  logical_offset.seq.size() * logical_offset.cnt <= 1 ) {
        // Only one offset in logical_offset, just check that one
        // Note that 5, [2]^1 and 5, []^0 are the same, they represent only 5
        //mlog(IDX_WARN, "check the only one");
        pos = 0;
        return isContain(offset, logical_offset.init, length.getValByPos(0));
    }

    if ( logical_offset.init == offset ) {
        //check the init separately from the matrix
        //mlog(IDX_WARN, "Hit the init");
        pos = 0;
        return isContain(offset, logical_offset.init, length.getValByPos(0));
    }

    assert (delta_sum > 0); //let's not handl this at this time. TODO:

    off_t roffset = offset - logical_offset.init; //logical offset starts from init
    off_t col = roffset % delta_sum; 
    off_t row = roffset / delta_sum; 

    //oss.str("");
    //oss<<"col:"<<col<<"row:"<<row;
    //mlog(IDX_WARN, "%s", oss.str().c_str());

    //cout << "col:" << col << endl;
    //cout << "row:" << row << endl;

    if ( row >= logical_offset.cnt ) {
        // logical is very large.
        // check the last offset
        //
        // note that there are totally cnt*seq.size() offsets
        // in this class
        // they are:
        // [init][init+d0][init+d0+d1]...[init+(d0+d1+..+dp)*cnt-dp]
        // [init+(d0+d1+..+dp)*cnt] is the 'last+1' offset
        int last_pos = logical_offset.cnt * logical_offset.seq.size() - 1;
        off_t off = logical_offset.getValByPos(last_pos);
        off_t len = length.getValByPos(last_pos);
        pos = last_pos;
        //mlog(IDX_WARN, "check the last %d", pos);
        return isContain(offset, off, len);
    } else {
        off_t sum = 0;
        int col_pos;
        
        for ( col_pos = 0;
              sum <= col;
              col_pos++ )
        {
            sum += logical_offset.seq[col_pos];
        }
        
        col_pos--;  //seq[0~col_pos] = sum

        /*      
        int chkpos_in_matric = col_pos - 1
                               + row*logical_offset.seq.size() ;
        int chkpos_in_logical_off = chkpos_in_matric + 1;
        */
        
        pos = col_pos + row*logical_offset.seq.size() ;
        //oss.str("");
        //oss << "Inside." <<  "col_pos:" << col_pos << endl;
        //oss << "chkpos_in_matric:" << chkpos_in_matric << endl;
        //oss << "chkpos_in_logical_off:" << chkpos_in_logical_off << endl;
        //mlog(IDX_WARN, "%s", oss.str().c_str());
        return isContain(offset, 
                         logical_offset.getValByPos(pos),
                         length.getValByPos(pos));
    }
}

// pos has to be in the range
inline
off_t IdxSigUnit::getValByPos( const int &pos  ) 
{
    off_t locval = 0;
    int mpos;
    int col, row;
    off_t seqsum;
    off_t val = -1;

    if ( pos == 0 ) {
	    return init;
	}

    /* 
     * The caller should make sure this
     *
    if ( seq.size() == 0 || cnt == 0 || pos < 0 || pos >= seq.size()*cnt ) {
        // that's nothing in seq and you are requesting 
        // pos > 0. Sorry, no answer for that.
        ostringstream oss;
        oss << "In " << __FUNCTION__ << 
            " Request out of range. Pos is " << pos << endl;
        mlog (IDX_ERR, "%s", oss.str().c_str());
        assert(0); // Make it hard for the errors
    }
    */

    locval = init;
	mpos = pos - 1; //the position in the matrix
	col = mpos % seq.size();
    //cout << "col" << col << endl;
	row = mpos / seq.size();
    //cout << "row" << row << endl;

	if ( ! (row < cnt) ) {
        assert(0); // Make it hard for the errors
	}
	seqsum = sumVector(seq);
    //cout << "seqsum" << seqsum << endl;
	locval += seqsum * row;
	
	int i = 0;
	while ( i <= col ) {
		locval += seq[i];
        i++;
	}
    
    val = locval;
	return val;
}


inline
off_t sumVector( vector<off_t> seq )
{
    vector<off_t>::const_iterator iiter;
    
    off_t sum = 0;
    for ( iiter = seq.begin() ;
          iiter != seq.end() ;
          iiter++ )
    {
        sum += *iiter;
    }
    
    return sum;
}

inline bool isContain( off_t off, off_t offset, off_t length )
{
    //ostringstream oss;
    //oss << "isContain(" << off << ", " << offset << ", " << length << ")" <<  endl;
    //mlog(IDX_WARN, "%s", oss.str().c_str());
    return ( offset <= off && off < offset+length );
}




#endif

