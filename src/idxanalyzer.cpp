/*
 * This is where the pattern recognition and handling happen. 
 * Somewhere in the code you may see the term "signature", which means 
 * "pattern" actually. 
 */

#include "idxanalyzer.h"
#include "Util.h"
#include "plfs_private.h"

#include <algorithm>

// for debugging
string printIdxEntries( vector<IdxSigEntry> &idx_entry_list )
{
    vector<IdxSigEntry>::iterator iter;

    //cout << "this is printIdxEntries" << endl;

    ostringstream showstr;
    for ( iter = idx_entry_list.begin();
            iter != idx_entry_list.end();
            iter++ )
    {
        showstr << iter->show();
    }
    return showstr.str();
}

string printVector( vector<off_t> vec )
{
    ostringstream oss;
    oss << "PrintVector: " ;
    vector<off_t>::const_iterator it;
    for ( it =  vec.begin() ;
          it != vec.end() ;
          it++ )
    {
        oss << *it << "," ;
    }
    oss << endl;
    return oss.str();
}


// delta[i] = seq[i]-seq[i-1]
vector<off_t> buildDeltas( vector<off_t> seq ) 
{
    vector<off_t>::iterator it;
    vector<off_t> deltas;
    //ostringstream oss;
    for ( it = seq.begin() ; it != seq.end() ; it++ )
    {
        if ( it > seq.begin() ) {
            deltas.push_back( *it - *(it-1) );
            //oss << (*it - *(it-1)) << "-";
        }
    }
    //mlog(IDX_WARN, "deltas:%s", oss.str().c_str());
    //cout << "in builddeltas: " << seq.size() << " " << deltas.size() << endl; 
    return deltas;
}

// It recognizes the patterns
// and returens a list of pattern entries.
IdxSigEntryList IdxSignature::generateIdxSignature(
        vector<HostEntry> &entry_buf, 
        int proc) 
{
    vector<off_t> logical_offset, length, physical_offset; 
    vector<off_t> logical_offset_delta, 
                  length_delta, 
                  physical_offset_delta;
    IdxSigEntryList entrylist;
    vector<HostEntry>::const_iterator iter;

    // handle one process at a time.
    for ( iter = entry_buf.begin() ; 
            iter != entry_buf.end() ;
            iter++ )
    {
        if ( iter->id != proc ) {
            continue;
        }

        logical_offset.push_back(iter->logical_offset);
        length.push_back(iter->length);
        physical_offset.push_back(iter->physical_offset);
    }
   
    // only for debuggin. 
    if ( !(logical_offset.size() == length.size() &&
            length.size() == physical_offset.size()) ) {
        ostringstream oss;
        oss << "logical_offset.size():" << logical_offset.size() 
            << "length.size():" << length.size()
            << "physical_offset.size():" << physical_offset.size() << endl;
        mlog(IDX_WARN, "sizes should be equal. %s", oss.str().c_str());
        exit(-1);
    }


    // Get the patterns of logical offsets first
    SigStack<IdxSigUnit> offset_sig = generateComplexPatterns(logical_offset);

    //Now, go through offset_sig one by one and build the IdxSigEntry s
    vector<IdxSigEntry>idx_entry_list;
    vector<IdxSigUnit>::const_iterator stack_iter;

    /*
    ostringstream oss;
    oss << "offset patterns:" << offset_sig.show() << endl;
    oss << "offsets: " << printVector( logical_offset ) << endl;
    mlog(IDX_WARN, "%s", oss.str().c_str());
    */

    int range_start = 0, range_end; //the range currently processing
    for (stack_iter = offset_sig.begin();
            stack_iter != offset_sig.end();
            stack_iter++ )
    {
        //cout << stack_iter->init << " " ;
        IdxSigEntry idx_entry;
        range_end = range_start 
                    + max( int(stack_iter->cnt * stack_iter->seq.size()), 1 );  
//        mlog(IDX_WARN, "range_start:%d, range_end:%d, logical_offset.size():%d",
//                range_start, range_end, logical_offset.size());
        assert( range_end <= logical_offset.size() );

        idx_entry.original_chunk = proc;
        idx_entry.logical_offset = *stack_iter;
       
        // Find the corresponding patterns of lengths
        vector<off_t> length_seg(length.begin()+range_start,
                                 length.begin()+range_end);
        idx_entry.length = generateComplexPatterns(length_seg);

        // Find the corresponding patterns of physical offsets
        vector<off_t> physical_offset_seg( physical_offset.begin()+range_start,
                                           physical_offset.begin()+range_end);
        idx_entry.physical_offset = generateComplexPatterns(physical_offset_seg);
        
        idx_entry_list.push_back( idx_entry);

        range_start = range_end;
    }
    entrylist.append(idx_entry_list);
    //printIdxEntries(idx_entry_list);
    return entrylist;
}


// *** this function is no longer in use ***
//find out pattern of a number sequence 
void IdxSignature::discoverPattern(  vector<off_t> const &seq )
{
    vector<off_t>::const_iterator p_lookahead_win; // pointer(iterator) to the lookahead window
    PatternStack<PatternUnit> pattern_stack;

    p_lookahead_win = seq.begin();
    pattern_stack.clear();

    //cout << endl << "this is discoverPattern() :)" << endl;

    while ( p_lookahead_win != seq.end() ) {
        //lookahead window is not empty
        Tuple cur_tuple = searchNeighbor( seq, p_lookahead_win );
        //cur_tuple.show();
        if ( cur_tuple.isRepeatingNeighbor() ) {
            if ( pattern_stack.isPopSafe( cur_tuple.length ) ) {
                //safe
                pattern_stack.popElem( cur_tuple.length );

                vector<off_t>::const_iterator first, last;
                first = p_lookahead_win;
                last = p_lookahead_win + cur_tuple.length;

                PatternUnit pu;
                pu.seq.assign(first, last);
                pu.cnt = 2;

                pattern_stack.push( pu );
                p_lookahead_win += cur_tuple.length;
            } else {
                //unsafe
                PatternUnit pu = pattern_stack.top();

                if ( pu.seq.size() == cur_tuple.length ) {
                    //the subseq in lookahead window repeats
                    //the top pattern in stack
                    pu.cnt++;
                    pattern_stack.popPattern();
                    pattern_stack.push(pu);

                    p_lookahead_win += cur_tuple.length;
                } else {
                    //cannot pop out cur_tuple.length elems without
                    //totally breaking any pattern.
                    //So we simply add one elem to the stack
                    PatternUnit pu2;
                    pu2.seq.push_back( *p_lookahead_win );
                    pu2.cnt = 1;
                    pattern_stack.push(pu2);
                    p_lookahead_win++;
                }
            }


        } else {
            //(0,0,x)
            PatternUnit pu;
            pu.seq.push_back(cur_tuple.next_symbol);
            pu.cnt = 1;

            pattern_stack.push(pu); 
            p_lookahead_win++;
        }
        //pattern_stack.show();
    }

}

// This is the key function of LZ77. 
// The input is a sequence of number and 
// it returns patterns like: i,(x,x,x,..)^r, ...
SigStack<IdxSigUnit>
IdxSignature::generateComplexPatterns( vector<off_t> inits )
{
    //mlog(IDX_WARN, "Entering %s. inits: %s", 
    //     __FUNCTION__, printVector(inits).c_str());
    vector<off_t> deltas = buildDeltas(inits);
    SigStack<IdxSigUnit> pattern, patterntrim;

    pattern = findPattern( deltas ); // find repeating pattern in deltas

    // Put the init back into the patterns
    int pos = 0;
    vector<IdxSigUnit>::iterator it;
    for ( it = pattern.the_stack.begin() ;
          it != pattern.the_stack.end() ;
          it++ )
    {
        it->init = inits[pos];
        pos += it->seq.size() * it->cnt;
    }


    // handle the missing last
    if ( ! inits.empty() ) {
        if ( ! pattern.the_stack.empty() 
             && pattern.the_stack.back().seq.size() == 1 
             && pattern.the_stack.back().init + 
                pattern.the_stack.back().seq[0] * pattern.the_stack.back().cnt 
                 == inits.back() ) 
        {
            pattern.the_stack.back().cnt++;
        } else {        
            IdxSigUnit punit;
            punit.init = inits.back();
            punit.cnt = 1;
            pattern.push(punit);
        }
    }

    // combine the unecessary single patterns
    for ( it = pattern.the_stack.begin() ;
          it != pattern.the_stack.end() ;
          it++ )
    {
        ostringstream oss;
        oss << it->show() ;
//        mlog(IDX_WARN, "TRIM:%s", oss.str().c_str());
        
        if ( it != pattern.the_stack.begin()
             && it->cnt * it->seq.size() <= 1
             && patterntrim.the_stack.back().seq.size() == 1
             && patterntrim.the_stack.back().cnt > 1
             && patterntrim.the_stack.back().init
                + patterntrim.the_stack.back().seq[0]
                * patterntrim.the_stack.back().cnt == it->init
             ) 
        {
            // it can be represented by the last pattern.
//            mlog(IDX_WARN, "in back++");
            patterntrim.the_stack.back().cnt++;
        } else {
//            mlog(IDX_WARN, "in push");
            patterntrim.push(*it);
        }
    }
   
    
    /*
    ostringstream oss;
    oss << pattern.show() << endl; 
    mlog(IDX_WARN, "Leaving %s:{%s}", __FUNCTION__, oss.str().c_str());
    */

    return patterntrim;
}


// It returns repeating patterns of a sequence of numbers. (x,x,x...)^r
SigStack<IdxSigUnit> IdxSignature::findPattern( vector<off_t> deltas )
{
    // pointer(iterator) to the lookahead window, both should move together
//    mlog(IDX_WARN, "Entering %s", __FUNCTION__);
//    mlog(IDX_WARN, "deltas: %s", printVector(deltas).c_str());
    vector<off_t>::const_iterator p_lookahead_win;
    SigStack<IdxSigUnit> pattern_stack;

    p_lookahead_win = deltas.begin();
    pattern_stack.clear();

    while ( p_lookahead_win < deltas.end() ) {
        //lookahead window is not empty
//        mlog(IDX_WARN, "window position:%d", p_lookahead_win - deltas.begin());
        Tuple cur_tuple = searchNeighbor( deltas, p_lookahead_win );
        mlog(IDX_WARN, "%s", cur_tuple.show().c_str());
        if ( cur_tuple.isRepeatingNeighbor() ) {
            if ( pattern_stack.isPopSafe( cur_tuple.length ) ) {
//                mlog(IDX_WARN, "SAFE" );
                //safe
                //pop out elements without breaking existing patterns
                pattern_stack.popElem( cur_tuple.length );

                vector<off_t>::const_iterator first, last;
                first = p_lookahead_win;
                last = p_lookahead_win + cur_tuple.length;

                IdxSigUnit pu;
                pu.seq.assign(first, last);
                pu.cnt = 2;

                pattern_stack.push( pu );
                p_lookahead_win += cur_tuple.length;
            } else {
                //unsafe
//                mlog(IDX_WARN, "UNSAFE" );
                IdxSigUnit pu = pattern_stack.top();

                if ( cur_tuple.length % pu.seq.size() == 0
                     && cur_tuple.length <= pu.seq.size() * pu.cnt ) {
//                    mlog(IDX_WARN, "-REPEATING LAST Pattern");
                    //the subseq in lookahead window repeats
                    //the top pattern in stack.
                    //initial remains the same.
                    pu.cnt += cur_tuple.length / pu.seq.size() ;
                    pattern_stack.popPattern();
                    pattern_stack.push(pu);

                    p_lookahead_win += cur_tuple.length;
                } else {
//                    mlog(IDX_WARN, "-Just Pust it in");
                    //cannot pop out cur_tuple.length elems without
                    //totally breaking any pattern.
                    //So we simply add one elem to the stack
                    IdxSigUnit pu;
                    pu.seq.push_back( *p_lookahead_win );
                    pu.cnt = 1;
                    pattern_stack.push(pu);
                    p_lookahead_win++;
                }
            }
        } else {
            //(0,0,x), nothing repeats
            IdxSigUnit pu;
            pu.seq.push_back(cur_tuple.next_symbol);
            pu.cnt = 1;

            pattern_stack.push(pu); 
            p_lookahead_win++;
        }
        pattern_stack.the_stack.back().compressRepeats();
//        mlog(IDX_WARN, "LOOP: %s", pattern_stack.show().c_str());
    }
   
//    mlog(IDX_WARN, "Leaving %s:%s", __FUNCTION__, pattern_stack.show().c_str());
    return pattern_stack;

}


// *** this function is no longer in use *** 
//find out pattern of a number sequence(deltas) with its
//original sequence
//if seq and orig have the same sizes.
//  the function returns pattern representing all orig numbers.
//else if orig has one more than seq (including seq.size()==0)
//  the function returns pattern representing all orig numbers, 
//  with the last orig num with seq.size()==0
//else
//  error
SigStack<IdxSigUnit> IdxSignature::discoverSigPattern( vector<off_t> const &seq,
        vector<off_t> const &orig )
{
    // pointer(iterator) to the lookahead window, bot should move together
    vector<off_t>::const_iterator p_lookahead_win, 
        p_lookahead_win_orig; 
    SigStack<IdxSigUnit> pattern_stack;

    p_lookahead_win = seq.begin();
    p_lookahead_win_orig = orig.begin();
    pattern_stack.clear();

    if (! (seq.size() == orig.size()
            || seq.size() + 1 == orig.size() ) )
    {
        ostringstream oss;
        oss << "seq.size():" << seq.size()
            << " orig.size():" << orig.size() << endl;
        mlog(IDX_ERR, "discoverSigPattern() needs to be used with "
                "seq.size==orig.size or seq.size+1==orig.size. \n %s", 
                oss.str().c_str() );
        exit(-1);
    }

    //cout << endl << "this is discoverPattern() :)" << endl;
   
    //TODO:
    //There's bug in handling the case of only one entry.
    //And whether 1,(3,4)^2 represnets five or four numbers.
    //Go back to handle this when protoc buffer is integrated.
    /*
    cout << "seq.size(): " << seq.size() << "orig.size():" << orig.size() << endl;
    assert(seq.size() == (orig.size()-1));

    //for the case there is only one entry
    if ( seq.size() == 0 ) {
        IdxSigUnit pu;
        pu.init = *p_lookahead_win_orig;
        pu.cnt = 0;
    }
    */    



    //TODO: WHY p_lookahead_win != seq.end() is a dead loop????
    while ( p_lookahead_win < seq.end() ) {
        //lookahead window is not empty
        Tuple cur_tuple = searchNeighbor( seq, p_lookahead_win );
        //cur_tuple.show();
        if ( cur_tuple.isRepeatingNeighbor() ) {
            if ( pattern_stack.isPopSafe( cur_tuple.length ) ) {
                //safe
                pattern_stack.popElem( cur_tuple.length );

                vector<off_t>::const_iterator first, last;
                first = p_lookahead_win;
                last = p_lookahead_win + cur_tuple.length;

                IdxSigUnit pu;
                pu.seq.assign(first, last);
                pu.cnt = 2;
                pu.init = *(p_lookahead_win_orig - cur_tuple.length);

                pattern_stack.push( pu );
                p_lookahead_win += cur_tuple.length;
                p_lookahead_win_orig += cur_tuple.length;
            } else {
                //unsafe
                IdxSigUnit pu = pattern_stack.top();

                if ( cur_tuple.length % pu.seq.size()
                     && cur_tuple.length <= pu.seq.size() * pu.cnt ) {
                    //the subseq in lookahead window repeats
                    //the top pattern in stack.
                    //initial remains the same.
                    pu.cnt += cur_tuple.length / pu.seq.size() ;
                    pattern_stack.popPattern();
                    pattern_stack.push(pu);
                    pu.init = *p_lookahead_win_orig; //should delete this. keep if only for 
                    //tmp debug.

                    p_lookahead_win += cur_tuple.length;
                    p_lookahead_win_orig += cur_tuple.length;
                } else {
                    //cannot pop out cur_tuple.length elems without
                    //totally breaking any pattern.
                    //So we simply add one elem to the stack
                    IdxSigUnit pu;
                    pu.seq.push_back( *p_lookahead_win );
                    pu.init = *p_lookahead_win_orig;
                    pu.cnt = 1;
                    pattern_stack.push(pu);
                    p_lookahead_win++;
                    p_lookahead_win_orig++;
                }
            }
        } else {
            //(0,0,x)
            IdxSigUnit pu;
            pu.seq.push_back(cur_tuple.next_symbol);
            pu.init = *p_lookahead_win_orig;
            pu.cnt = 1;

            pattern_stack.push(pu); 
            p_lookahead_win++;
            p_lookahead_win_orig++;
        }
    }
   
    if ( p_lookahead_win_orig < orig.end() ) {
        assert(p_lookahead_win_orig + 1 == orig.end());
        IdxSigUnit pu;
        pu.init = *p_lookahead_win_orig;
        pu.cnt = 0;

        pattern_stack.push(pu); 
    }
   
    SigStack<IdxSigUnit> pattern_stack_compressed;
    vector<IdxSigUnit>::iterator it;
    for ( it = pattern_stack.the_stack.begin();
          it != pattern_stack.the_stack.end();
          it++ )
    {
        it->compressRepeats();
        if (pattern_stack_compressed.the_stack.empty()) {
            mlog(IDX_WARN, "Empty");
            pattern_stack_compressed.the_stack.push_back(*it);
        } else {
            bool ret;
            ret = pattern_stack_compressed.the_stack.back().append(*it);
            if (ret == false) {
                pattern_stack_compressed.the_stack.push_back(*it);
            }
        }
        //ostringstream oss;
        //oss << pattern_stack_compressed.show();
        //mlog(IDX_WARN, "%s", oss.str().c_str());
    }
   
    

    if ( pattern_stack.size() != orig.size() ) {
        ostringstream oss;
        oss<< "pattern_stack.size() != orig.size() in"
               << __FUNCTION__ << pattern_stack.size() 
               << "," << orig.size() << endl;
        oss << "seq.size():" << seq.size() << endl;
        oss << pattern_stack.show() << endl;
        vector<off_t>::const_iterator it;
        for ( it = orig.begin();
              it != orig.end();
              it++ )
        {
            oss << *it << ",";
        }
        oss << endl;
        mlog(IDX_ERR, "%s", oss.str().c_str());
        exit(-1);
    }

    return pattern_stack_compressed;
}

Tuple IdxSignature::searchNeighbor( vector<off_t> const &seq,
        vector<off_t>::const_iterator p_lookahead_win ) 
{
    vector<off_t>::const_iterator i;     
    int j;
    //cout << "------------------- I am in searchNeighbor() " << endl;

    //i goes left util the begin or reaching window size
    i = p_lookahead_win;
    int remain = seq.end() - p_lookahead_win;
    while ( i != seq.begin() 
            && (p_lookahead_win - i) < win_size
            && (p_lookahead_win - i) < remain ) {
        i--;
    }
    //termination: i == seq.begin() or distance == win_size

    /*
    //print out search buffer and lookahead buffer
    //cout << "search buf: " ;
    vector<off_t>::const_iterator k;
    for ( k = i ; k != p_lookahead_win ; k++ ) {
    cout << *k << " ";
    }
    cout << endl;

    cout << "lookahead buf: " ;
    vector<off_t>::const_iterator p;
    p = p_lookahead_win;
    for ( p = p_lookahead_win ; 
    p != seq.end() && p - p_lookahead_win < win_size ; 
    p++ ) {
    cout << *p << " ";
    }
    cout << endl;
    */

    //i points to a element in search buffer where matching starts
    //j is the iterator from the start to the end of search buffer to compare
    //smarter algorithm can be used better time complexity, like KMP.
    for ( ; i != p_lookahead_win ; i++ ) {
        int search_length = p_lookahead_win - i;
        for ( j = 0 ; j < search_length ; j++ ) {
            if ( *(i+j) != *(p_lookahead_win + j) ) {
                break;
            }
        }
        if ( j == search_length ) {
            //found a repeating neighbor
            return Tuple(search_length, search_length, 
                    *(p_lookahead_win + search_length));
        }
    }

    //Cannot find a repeating neighbor
    return Tuple(0, 0, *(p_lookahead_win));
}

void IdxSigEntryList::append( vector<IdxSigEntry> &other ) 
{
    vector<IdxSigEntry>::iterator iter;
    for (iter = other.begin();
            iter != other.end();
            iter++ )
    {
        append(*iter, true);
    }

}

void IdxSigEntryList::append( IdxSigEntryList other ) 
{
    append(other.list);
}

void IdxSigEntryList::append( IdxSigEntry other, bool compress ) 
{
    if ( compress == false || list.empty() ) {
        list.push_back(other);
        return ;
    } else {
        if ( ! list.back().append(other) ) {
            list.push_back(other);
            return;
        }
    }
    return;
}

string 
IdxSigEntryList::show() 
{
    ostringstream showstr;
    showstr << printIdxEntries(list);

    vector<HostEntry>::const_iterator it;
    for ( it = messies.begin() ;
          it != messies.end() ;
          it++ ) 
    {
        double begin_timestamp = 0, end_timestamp = 0;
        begin_timestamp = (*it).begin_timestamp;
        end_timestamp  = (*it).end_timestamp;
        showstr  << setw(5)
            << (*it).id             << " w "
            << setw(16)
            << (*it).logical_offset << " "
            << setw(8) << (*it).length << " "
            << setw(16) << fixed << setprecision(16)
            << begin_timestamp << " "
            << setw(16) << fixed << setprecision(16)
            << end_timestamp   << " "
            << setw(16)
            << (*it).logical_tail() << " "
            << " [" << setw(10) << (*it).physical_offset << "]";
    }

    return showstr.str();
}

// [bodysize][type][body:[entry0][entry1]...]
void IdxSigEntryList::saveMessiesToFile(const int fd)
{
    header_t entrybodysize = sizeof(HostEntry)*messies.size();
    if ( entrybodysize == 0 ) {
        return;

    }
    char entrytype = 'M'; //means Messies

    Util::Writen(fd, &entrybodysize, sizeof(entrybodysize));
    Util::Writen(fd, &entrytype, sizeof(entrytype));

    if ( entrybodysize > 0 ) {
        Util::Writen(fd, &messies[0], entrybodysize);
    }
    return ;
}

// It appends to the buffer, to save time on copying
string &IdxSigEntryList::serializeMessies( string &buf )
{
    header_t entrybodysize = sizeof(HostEntry)*messies.size();
    char entrytype = 'M'; //means Messies

    appendToBuffer(buf, &entrybodysize, sizeof(entrybodysize));
    appendToBuffer(buf, &entrytype, sizeof(entrytype));

    if ( entrybodysize > 0 ) {
        appendToBuffer(buf, &messies[0], entrybodysize);
    }   
    return buf;
}


void IdxSigEntryList::deSerializeMessies( string &buf )
{
    header_t entrybodysize;
    char entrytype; //means Messies
    int cur = 0;

    readFromBuf(buf, &entrybodysize, cur, sizeof(entrybodysize));
    readFromBuf(buf, &entrytype, cur, sizeof(entrytype));
    messies.resize( entrybodysize/sizeof(HostEntry) );

    if ( entrybodysize > 0 ) {
        readFromBuf(buf, &messies[0], cur, entrybodysize);
    }
    return;
}


void IdxSigEntryList::saveListToFile(const int fd)
{
    string buf = serialize();
    if ( buf.size() > 0 ) {
        Util::Writen(fd, &buf[0], buf.size());
    }
}



void IdxSigEntryList::saveToFile(const int fd)
{
    saveListToFile(fd);
    saveMessiesToFile(fd);
}


void IdxSigEntryList::clear()
{
    list.clear();
    messies.clear();
}


void appendToBuffer( string &to, const void *from, const int size )
{
    if ( size > 0 ) { //make it safe
        to.append( (char *)from, size );
    }
}

//Note that this function will increase start
void readFromBuf( string &from, void *to, int &start, const int size )
{
    //'to' has to be treated as plain memory
    memcpy(to, &from[start], size);
    start += size;
}

//Serialiezd IdxSigUnit: [head:bodysize][body]
header_t IdxSigUnit::bodySize()
{
    header_t totalsize;
    totalsize = sizeof(init) //init
                + sizeof(cnt) //cnt
                + sizeof(header_t) //length of seq size header
                + seq.size()*sizeof(off_t);
    return totalsize;
}

// check if this follows other and merge
// has to satisfy two:
// 1. seq are exactly the same OR (repeating and the same, size can be diff)
//    (3,3,3)==(3,3,3)             (3,3,3)==(3), (3,3,3)==()
// 2. AND init1 + sum of deltas == init2
// return true if appended successfully
bool IdxSigUnit::append( IdxSigUnit &other )
{

//    mlog(IDX_WARN, "in %s", __FUNCTION__);

    if ( this->isSeqRepeating() 
        && other.isSeqRepeating() )
    {
        if ( this->size() > 1 && other.size() > 1 ) {
            //case 1. both has size > 1
            if ( this->seq[0] == other.seq[0] 
                 && this->init + this->seq[0]*this->size() == other.init ) {
                int newsize = this->size() + other.size();
                this->seq.clear();
                this->seq.push_back(other.seq[0]);
                this->cnt = newsize;
                return true;
            } else {
                return false;
            }               
        } else if ( this->size() == 1 && other.size() == 1 ) {
            //case 2. both has size == 1
            //definitely follows
            this->seq.clear();
            this->seq.push_back(other.init - this->init);
            this->cnt = 2; //has two now
            return true;
        } else if ( this->size() == 1 && other.size() > 1 ) {
            if ( other.init - this->init == other.seq[0] ) {
                int newsize = this->size() + other.size();
                this->seq.clear();
                this->seq.push_back(other.seq[0]);
                this->cnt = newsize;
                return true;
            } else {
                return false;
            }
        } else if ( this->size() > 1 && other.size() == 1) {
            if ( this->init + this->seq[0]*this->size() == other.init ) {
                int newsize = this->size() + other.size();
                off_t tmp = this->seq[0];
                this->seq.clear();
                this->seq.push_back(tmp);
                this->cnt = newsize;
                return true;
            } else {
                return false;
            }
        }
    } else {
        return false;  //TODO:should handle this case
    }
}

// (3,3,3)^4 is repeating
// (0,0)^0 is also repeating
bool IdxSigUnit::isSeqRepeating()
{
    vector<off_t>::iterator it;
    bool allrepeat = true;
    for ( it = seq.begin();
          it != seq.end();
          it++ )
    {
        if ( it != seq.begin()
             && *it != *(it-1) ) {
            allrepeat = false;
            break;
        }
    }
    return allrepeat;
}

void IdxSigUnit::compressRepeats()
{
    if ( isSeqRepeating() && size() > 1 ) {
        cnt = size();
        off_t tmp = seq[0];
        seq.clear();
        seq.push_back(tmp);
    }
}


string 
IdxSigUnit::serialize()
{
    string buf; //let me put it in string and see if it works
    header_t seqbodysize;
    header_t totalsize;

    totalsize = bodySize(); 
    
    appendToBuffer(buf, &totalsize, sizeof(totalsize));
    appendToBuffer(buf, &init, sizeof(init));
    appendToBuffer(buf, &cnt, sizeof(cnt));
    seqbodysize = seq.size()*sizeof(off_t);
    appendToBuffer(buf, &(seqbodysize), sizeof(header_t));
    if (seqbodysize > 0 ) {
        appendToBuffer(buf, &seq[0], seqbodysize);
    }
    return buf;
}

//This input buf should be [data size of the followed data][data]
void 
IdxSigUnit::deSerialize(string buf)
{
    header_t totalsize;
    int cur_start = 0;
    header_t seqbodysize;

    readFromBuf(buf, &totalsize, cur_start, sizeof(totalsize));
    readFromBuf(buf, &init, cur_start, sizeof(init));
    readFromBuf(buf, &cnt, cur_start, sizeof(cnt));
    readFromBuf(buf, &seqbodysize, cur_start, sizeof(header_t));
    if ( seqbodysize > 0 ) {
        seq.resize(seqbodysize/sizeof(off_t));
        readFromBuf(buf, &seq[0], cur_start, seqbodysize); 
    }
}

//byte size is in [bodysize][data]
//it is the size of data
int IdxSigEntry::bodySize()
{
    int totalsize = 0;
    totalsize += sizeof(original_chunk);
    totalsize += sizeof(new_chunk_id);
    totalsize += sizeof(begin_timestamp);
    totalsize += sizeof(end_timestamp);
    totalsize += sizeof(header_t) * 3; //the header size of the following 
    totalsize += logical_offset.bodySize();
    totalsize += length.bodySize();
    totalsize += physical_offset.bodySize();

    return totalsize;
}

string IdxSigEntry::serialize()
{
    header_t totalsize = 0;
    string buf, tmpbuf;
    header_t datasize;
    
    totalsize = bodySize();
    //cout << "IdxSigEntry totalsize put in: " << totalsize << endl;
    appendToBuffer(buf, &totalsize, sizeof(totalsize));
    appendToBuffer(buf, &original_chunk, sizeof(original_chunk));
    appendToBuffer(buf, &new_chunk_id, sizeof(new_chunk_id));
    appendToBuffer(buf, &begin_timestamp, sizeof(begin_timestamp));
    appendToBuffer(buf, &end_timestamp, sizeof(end_timestamp));
    //cout << "IdxSigEntry original_chunk put in: " << original_chunk << endl; 
    
    //this tmpbuf includes [data size][data]
    tmpbuf = logical_offset.serialize(); 
    appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());
    
    tmpbuf = length.serialize();
    appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());

    tmpbuf = physical_offset.serialize();
    appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());

    return buf;
}



void IdxSigEntry::deSerialize(string buf)
{
    header_t totalsize = 0; 
    int cur_start = 0;
    header_t datasize = 0;
    string tmpbuf;


    readFromBuf(buf, &totalsize, cur_start, sizeof(totalsize));
    //cout << "IdxSigEntry totalsize read out: " << totalsize << endl;
    
    readFromBuf(buf, &original_chunk, cur_start, sizeof(original_chunk));
    //cout << "IdxSigEntry id read out: " << id << endl; 
    
    readFromBuf(buf, &new_chunk_id, cur_start, sizeof(new_chunk_id));
    readFromBuf(buf, &begin_timestamp, cur_start, sizeof(begin_timestamp));
    readFromBuf(buf, &end_timestamp, cur_start, sizeof(end_timestamp));
   
    tmpbuf.clear();
    readFromBuf(buf, &datasize, cur_start, sizeof(datasize));
    if ( datasize > 0 ) {
        int headanddatasize = sizeof(datasize) + datasize;
        tmpbuf.resize(headanddatasize);
        cur_start -= sizeof(datasize);
        readFromBuf(buf, &tmpbuf[0], cur_start, headanddatasize); 
    }
    logical_offset.deSerialize(tmpbuf);
    //cout << "deSerialized logical offset data size: " << datasize << endl;
    
    tmpbuf.clear();
    readFromBuf(buf, &datasize, cur_start, sizeof(datasize));
    if ( datasize > 0 ) {
        int headanddatasize = sizeof(datasize) + datasize;
        tmpbuf.resize(headanddatasize);
        cur_start -= sizeof(datasize);
        readFromBuf(buf, &tmpbuf[0], cur_start, headanddatasize); 
    }
    length.deSerialize(tmpbuf);

    tmpbuf.clear();
    readFromBuf(buf, &datasize, cur_start, sizeof(datasize));
    if ( datasize > 0 ) {
        int headanddatasize = sizeof(datasize) + datasize;
        tmpbuf.resize(headanddatasize);
        cur_start -= sizeof(datasize);
        readFromBuf(buf, &tmpbuf[0], cur_start, headanddatasize); 
    }
    physical_offset.deSerialize(tmpbuf);
}

string IdxSigEntryList::serialize()
{
    header_t bodysize, realbodysize = 0;
    char entrytype = 'P'; // means Pattern
    string buf;
    vector<IdxSigEntry>::iterator iter;
    
    bodysize = bodySize();

    appendToBuffer(buf, &bodysize, sizeof(bodysize));
    appendToBuffer(buf, &entrytype, sizeof(entrytype));
    
    //cout << "list body put in: " << bodysize << endl;

    for ( iter = list.begin() ;
          iter != list.end() ;
          iter++ )
    {
        string tmpbuf;
        tmpbuf = iter->serialize();
        if ( tmpbuf.size() > 0 ) {
            appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());
        }
        realbodysize += tmpbuf.size();
    }
    assert(realbodysize == bodysize);
    //cout << realbodysize << "==" << bodysize << endl;

    return buf;
}

void IdxSigEntryList::deSerialize(string buf)
{
    header_t bodysize, bufsize;
    char entrytype;
    int cur_start = 0;

    list.clear();
    readFromBuf(buf, &bodysize, cur_start, sizeof(bodysize));
    readFromBuf(buf, &entrytype, cur_start, sizeof(entrytype));
    assert(entrytype == 'P'); 
    bufsize = buf.size();
    assert(bufsize == bodysize + sizeof(bodysize) + sizeof(entrytype));
    while ( cur_start < bufsize ) {
        header_t unitbodysize, sizeofheadandbody;
        string unitbuf;
        IdxSigEntry unitentry;

        readFromBuf(buf, &unitbodysize, cur_start, sizeof(unitbodysize));
        sizeofheadandbody = sizeof(unitbodysize) + unitbodysize; 
        unitbuf.resize(sizeofheadandbody);
        if ( unitbodysize > 0 ) {
            cur_start -= sizeof(unitbodysize);
            readFromBuf(buf, &unitbuf[0], cur_start, sizeofheadandbody);
        }
        unitentry.deSerialize(unitbuf);
        list.push_back(unitentry); //it is OK to push a empty entry
    }
    assert(cur_start==bufsize);
}

int IdxSigEntryList::bodySize()
{
    int bodysize = 0;
    vector<IdxSigEntry>::iterator iter;
    
    for ( iter = list.begin() ;
          iter != list.end() ;
          iter++ )
    {
        bodysize += iter->bodySize() + sizeof(header_t);
    }

    return bodysize;
}

void IdxSigEntryList::dumpMessies()
{
    vector<IdxSigEntry> nicepatterns;
    vector<IdxSigEntry>::iterator it;

    for ( it =  list.begin() ;
          it != list.end() ;
          it++ )
    {
        IdxSigEntry &patternentry = *it;
        if ( patternentry.logical_offset.size() <= 1 ) {
            //this is mess that had not been compressed
            //cout << "************************************************" << endl;
            //cout << patternentry.show() << endl;
            
            HostEntry hentry;
            hentry.logical_offset = patternentry.logical_offset.init;
            assert( patternentry.length.the_stack.size() > 0 );
            hentry.length = patternentry.length.the_stack.front().init;
            assert( patternentry.physical_offset.the_stack.size() > 0 );
            hentry.physical_offset = patternentry.physical_offset.the_stack.front().init;
            hentry.begin_timestamp = 0; //TODO add timestamp to Sig Entry
            hentry.end_timestamp = 0;
            hentry.id = patternentry.original_chunk;

            messies.push_back(hentry);
        } else {
            nicepatterns.push_back(patternentry);
        }
    }
    list = nicepatterns;
    //cout << "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF" << endl;
}

void IdxSigEntryList::messiesToPatterns()
{
    // Let me be lazy
    // Found out how what proc are in messies
    set<pid_t> procs;

    vector<HostEntry>::const_iterator iter;
    for ( iter = messies.begin() ;
          iter != messies.end() ;
          iter++ )
    {
        procs.insert( iter->id );
    }

    IdxSignature sigUtil; 

    set<pid_t>::const_iterator pid;
    for ( pid = procs.begin() ;
          pid != procs.end() ;
          pid++ )
    {
        append(sigUtil.generateIdxSignature(messies, *pid));
    }
    messies.clear();
}


//return number of elements in total
int PatternUnit::size() const 
{
    if ( cnt == 0 ) {
        return 1; //not repetition
    } else {
        return seq.size()*cnt;
    }
}

string 
PatternUnit::show() const
{
    vector<off_t>::const_iterator iter;
    ostringstream showstr;
    showstr << "( " ;
    for (iter = seq.begin();
            iter != seq.end();
            iter++ )
    {
        showstr << *iter << " ";
    }
    showstr << ") ^" << cnt << endl;
    return showstr.str();
}

string
IdxSigUnit::show() const
{
    ostringstream showstr;
    showstr << init << " ... ";
    showstr << PatternUnit::show();
    return showstr.str();
}



string IdxSigEntry::show()
{
    ostringstream showstr;

    showstr << "[" << original_chunk << "]" 
         << "[" << new_chunk_id << "]" << endl;
    showstr << "----Logical Offset----" << endl;
    showstr << logical_offset.show();
    
    vector<IdxSigUnit>::const_iterator iter2;

    showstr << "----Length----" << endl;
    for (iter2 = length.begin();
            iter2 != length.end();
            iter2++ )
    {
        showstr << iter2->show(); 
    }

    showstr << "----Physical Offset----" << endl;
    for (iter2 = physical_offset.begin();
            iter2 != physical_offset.end();
            iter2++ )
    {
        showstr << iter2->show(); 
    }
    showstr << "-------------------------------------" << endl;

    return showstr.str();
}

// At this time, we only append when:
// For logical off, length and physical off, each of them 
// has only one SigUnit
bool IdxSigEntry::append(IdxSigEntry &other)
{
    IdxSigEntry tmpentry = *this;

    if ( this->length.the_stack.size() == 1
         && this->physical_offset.the_stack.size() == 1
         && other.length.the_stack.size() == 1
         && other.physical_offset.the_stack.size() == 1 
         && tmpentry.logical_offset.append( other.logical_offset )
         && tmpentry.length.the_stack[0].append( other.length.the_stack[0] )
         && tmpentry.physical_offset.the_stack[0].append( 
                                          other.physical_offset.the_stack[0] ) )
    {
        *this = tmpentry;
        return true;
    } else {
        return false;
    }
}

int ContainerIdxSigEntry::bodySize()
{
    int totalsize = 0;
    totalsize += sizeof(begin_timestamp);
    totalsize += sizeof(end_timestamp);
    totalsize += sizeof(header_t) * 4; //the header size of the following 
    totalsize += sizeof(SigChunkMap) * chunkmap.size();
    totalsize += logical_offset.bodySize();
    totalsize += length.bodySize();
    totalsize += physical_offset.bodySize();

    return totalsize;
}

string ContainerIdxSigEntry::serialize()
{
    header_t totalsize = 0;
    string buf, tmpbuf;
    header_t datasize;
    header_t chunkmapsize = sizeof(SigChunkMap)*chunkmap.size();
    
    totalsize = bodySize();
    //cout << "IdxSigEntry totalsize put in: " << totalsize << endl;
    appendToBuffer(buf, &totalsize, sizeof(totalsize));
    appendToBuffer(buf, &begin_timestamp, sizeof(begin_timestamp));
    appendToBuffer(buf, &end_timestamp, sizeof(end_timestamp));
    
    appendToBuffer(buf, &chunkmapsize, sizeof(chunkmapsize));
    if ( chunkmapsize > 0 ) {
        appendToBuffer(buf, &chunkmap[0], chunkmapsize);
    }
    
    tmpbuf = logical_offset.serialize(); 
    appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());
    
    tmpbuf = length.serialize();
    appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());

    tmpbuf = physical_offset.serialize();
    appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());

    return buf;
}


void ContainerIdxSigEntry::deSerialize(string buf)
{
    header_t totalsize = 0; 
    int cur_start = 0;
    header_t datasize = 0;
    string tmpbuf;


    readFromBuf(buf, &totalsize, cur_start, sizeof(totalsize));
    //cout << "IdxSigEntry totalsize read out: " << totalsize << endl;
    
    readFromBuf(buf, &begin_timestamp, cur_start, sizeof(begin_timestamp));
    readFromBuf(buf, &end_timestamp, cur_start, sizeof(end_timestamp));
  
    header_t chunkmapsize;
    readFromBuf(buf, &chunkmapsize, cur_start, sizeof(chunkmapsize));
    
    if ( chunkmapsize > 0 ) {
        chunkmap.resize( chunkmapsize/sizeof(SigChunkMap) );
        readFromBuf(buf, &chunkmap[0], cur_start, chunkmapsize);
    }


    tmpbuf.clear();
    readFromBuf(buf, &datasize, cur_start, sizeof(datasize));
    if ( datasize > 0 ) {
        int headanddatasize = sizeof(datasize) + datasize;
        tmpbuf.resize(headanddatasize);
        cur_start -= sizeof(datasize);
        readFromBuf(buf, &tmpbuf[0], cur_start, headanddatasize); 
    }
    logical_offset.deSerialize(tmpbuf);
    //cout << "deSerialized logical offset data size: " << datasize << endl;
    
    tmpbuf.clear();
    readFromBuf(buf, &datasize, cur_start, sizeof(datasize));
    if ( datasize > 0 ) {
        int headanddatasize = sizeof(datasize) + datasize;
        tmpbuf.resize(headanddatasize);
        cur_start -= sizeof(datasize);
        readFromBuf(buf, &tmpbuf[0], cur_start, headanddatasize); 
    }
    length.deSerialize(tmpbuf);

    tmpbuf.clear();
    readFromBuf(buf, &datasize, cur_start, sizeof(datasize));
    if ( datasize > 0 ) {
        int headanddatasize = sizeof(datasize) + datasize;
        tmpbuf.resize(headanddatasize);
        cur_start -= sizeof(datasize);
        readFromBuf(buf, &tmpbuf[0], cur_start, headanddatasize); 
    }
    physical_offset.deSerialize(tmpbuf);   
}

string ContainerIdxSigEntry::show() const
{
    ostringstream showstr;

    showstr << "{";
    vector<SigChunkMap>::const_iterator it;
    for ( it =  chunkmap.begin() ;
          it != chunkmap.end() ;
          it++ )
    {
        showstr << "[" << it->original_chunk_id;
        showstr << "," << it->new_chunk_id << "]";
    }
    showstr << "}" << endl;

    showstr << "----Logical Offset----" << endl;
    showstr << logical_offset.show();
    
    vector<IdxSigUnit>::const_iterator iter2;

    showstr << "----Length----" << endl;
    for (iter2 = length.begin();
            iter2 != length.end();
            iter2++ )
    {
        showstr << iter2->show(); 
    }

    showstr << "----Physical Offset----" << endl;
    for (iter2 = physical_offset.begin();
            iter2 != physical_offset.end();
            iter2++ )
    {
        showstr << iter2->show(); 
    }
    showstr << "-------------------------------------" << endl;

    return showstr.str();
}


// [e2] is after [e1]: returen true
bool isAbut( ContainerIdxSigEntry e1, ContainerIdxSigEntry e2 )
{
//    mlog(IDX_WARN, "in %s", __FUNCTION__);
    if (    e1.logical_offset.seq.size() == 1
         && e2.logical_offset.seq.size() == 1
         && e1.logical_offset.seq[0] 
            == e2.logical_offset.seq[0]
         && e1.length.the_stack.size() == 1
         && e2.length.the_stack.size() == 1
         && e1.length.the_stack[0].seq.size() == 1 
         && e2.length.the_stack[0].seq.size() == 1 
         && e1.length.the_stack[0].seq[0] == 0 
         && e1.length.the_stack[0].seq[0] 
            == e2.length.the_stack[0].seq[0] 
         && e1.length.the_stack[0].cnt 
            == e2.length.the_stack[0].cnt
         && e1.physical_offset.the_stack.size() == 1
         && e2.physical_offset.the_stack.size() == 1
         && e1.physical_offset.the_stack[0].seq.size() == 1 
         && e2.physical_offset.the_stack[0].seq.size() == 1 
         && e1.physical_offset.the_stack[0].seq[0]
            == e2.physical_offset.the_stack[0].seq[0]
         && e1.physical_offset.the_stack[0].cnt
            == e2.physical_offset.the_stack[0].cnt )
    {
        // the lengths and physical offsets are identical now
        
        /*
        if ( e1.logical_offset.init 
              + e1.length.the_stack[0].init * e1.chunkmap.size()
             == e2.logical_offset.init )
        {
            // Only handle very simple case:
            //   e2 as the exactly same pattern, but different logical offset
            //   and they abut
            return 1;
        }
        */
//        ostringstream oss;

        off_t stride = e1.logical_offset.seq[0];
        if (stride <= 0) {
            return false;
        }
        off_t len = e1.length.the_stack[0].init;
        int e1mapsize = e1.chunkmap.size();
//        oss << "stride: " << stride 
//            << " len: " << len
//            << " e1mapsize: " << e1mapsize << endl;
//        mlog(IDX_WARN, "%s", oss.str().c_str());

        if ( len == 0 || stride%len != 0) {
            // len should be able to fill up the stride evenly
            return false;
        }

        int num_in_seg = stride/len;
        assert( num_in_seg > 0 );
        int row = e1mapsize / num_in_seg;
        int col = e1mapsize % num_in_seg;
//        oss.clear();
//        oss << "row: " << row 
//            << " col: " << col << endl;

        // allways keep e1 solid without hole
        // return 1 if e1 is solid and no hole between e1 and e2
        off_t newinit = e1.logical_offset.init 
                        + e1.logical_offset.seq[0] * e1.logical_offset.cnt * row;
//        oss << "newinit: " << newinit << endl;
//        mlog(IDX_WARN, "%s", oss.str().c_str());
        if ( newinit + len * col == e2.logical_offset.init ) {
//            mlog(IDX_WARN, "ITTTT is abut");
            return true;
        }
    }
//    mlog(IDX_WARN, "ITTTT is NOT abut");
    return false;
}

// This is where we build cross-proc patterns
void ContainerIdxSigEntryList::insertGlobal( const ContainerIdxSigEntry &entry )
{
    map<off_t, ContainerIdxSigEntry>::iterator before, after;
    after = listmap.lower_bound( entry.logical_offset.init );
  
//    ostringstream oss;
//    oss << "We are in " << __FUNCTION__ << endl;
//    oss << "to be insert:" << entry.show() << endl;
//    mlog(IDX_WARN, "%s", oss.str().c_str());

//    if ( after != listmap.end() ) {
//        mlog(IDX_WARN, "AFTER is :%s", after->second.show().c_str());
//    }

    if ( after != listmap.end() 
         &&  isAbut(entry, after->second) ) {
//        mlog(IDX_WARN, "abut after");
        ContainerIdxSigEntry con_entry = entry;
        con_entry.chunkmap.insert( con_entry.chunkmap.end(),
                                   after->second.chunkmap.begin(),
                                   after->second.chunkmap.end() );
        listmap.erase( after );
        listmap[con_entry.logical_offset.init] = con_entry;
        return;
    }

    if ( after != listmap.begin() && listmap.size() > 0 ) {
//        mlog(IDX_WARN, "There is a before");
        before = after;
        before--;

        if ( before != listmap.end() ) {
//            mlog(IDX_WARN, "BEFORe is :%s", before->second.show().c_str());
        }

        if ( isAbut( before->second, entry ) ) {
//            mlog(IDX_WARN, "abut before");
            before->second.chunkmap.insert( before->second.chunkmap.end(),
                                            entry.chunkmap.begin(),
                                            entry.chunkmap.end() );
            return;
        }
    }
//    mlog(IDX_WARN, "not abut at all");
    insertEntry( entry );
}

void ContainerIdxSigEntryList::insertEntry( const ContainerIdxSigEntry &entry )
{
    /*
    map<off_t, ContainerIdxSigEntry*>::iterator before, after;
    after = listmap.lower_bound( entry.logical_offset.init );
    
    ContainerIdxSigEntry *afterenry = after->second; //for short
    

    if ( after != listmap.begin() ) {
        before = after - 1;
    }

    list.push_back(entry);
    listmap[entry.logical_offset.init] = &list.back()
    */
    listmap[entry.logical_offset.init] = entry;
}


string ContainerIdxSigEntryList::show() const
{
    ostringstream oshowstr;
    map<off_t,ContainerIdxSigEntry>:: const_iterator it;
    for ( it =  listmap.begin() ; 
          it != listmap.end() ;
          it++ )
    {
        oshowstr << it->second.show();
    }
    return oshowstr.str();
}

int ContainerIdxSigEntryList::bodySize()
{
    int bodysize = 0;
    map<off_t,ContainerIdxSigEntry>::iterator iter;
    
    for ( iter = listmap.begin() ;
          iter != listmap.end() ;
          iter++ )
    {
        bodysize += iter->second.bodySize() + sizeof(header_t);
    }

    return bodysize;
}


string ContainerIdxSigEntryList::serialize()
{
    header_t bodysize, realbodysize = 0;
    char entrytype = 'C'; // means Container pattern
    string buf;
    map<off_t,ContainerIdxSigEntry>:: iterator iter;
    
    bodysize = bodySize();

    appendToBuffer(buf, &bodysize, sizeof(bodysize));
    appendToBuffer(buf, &entrytype, sizeof(entrytype));
    
    //cout << "list body put in: " << bodysize << endl;

    for ( iter = listmap.begin() ;
          iter != listmap.end() ;
          iter++ )
    {
        string tmpbuf;
        tmpbuf = iter->second.serialize();
        if ( tmpbuf.size() > 0 ) {
            appendToBuffer(buf, &tmpbuf[0], tmpbuf.size());
        }
        realbodysize += tmpbuf.size();
    }
    assert(realbodysize == bodysize);
    //cout << realbodysize << "==" << bodysize << endl;

    return buf;
}

void ContainerIdxSigEntryList::deSerialize(string buf)
{
    header_t bodysize, bufsize;
    char entrytype;
    int cur_start = 0;

    listmap.clear();
    readFromBuf(buf, &bodysize, cur_start, sizeof(bodysize));
    readFromBuf(buf, &entrytype, cur_start, sizeof(entrytype));
    assert(entrytype == 'C'); 
    bufsize = buf.size();
    assert(bufsize == bodysize + sizeof(bodysize) + sizeof(entrytype));
    while ( cur_start < bufsize ) {
        header_t unitbodysize, sizeofheadandbody;
        string unitbuf;
        ContainerIdxSigEntry unitentry;

        readFromBuf(buf, &unitbodysize, cur_start, sizeof(unitbodysize));
        sizeofheadandbody = sizeof(unitbodysize) + unitbodysize; 
        unitbuf.resize(sizeofheadandbody);
        if ( unitbodysize > 0 ) {
            cur_start -= sizeof(unitbodysize);
            readFromBuf(buf, &unitbuf[0], cur_start, sizeofheadandbody);
        }
        unitentry.deSerialize(unitbuf);
        insertEntry(unitentry); //it is OK to push a empty entry
    }
    assert(cur_start==bufsize);
}

void ContainerIdxSigEntryList::crossProcMerge()
{
    mlog(IDX_WARN, "In %s", __FUNCTION__);
    map<off_t, ContainerIdxSigEntry> tmpmap;
    tmpmap = listmap;
    listmap.clear();

    map<off_t, ContainerIdxSigEntry>::iterator it;
    for ( it =  tmpmap.begin() ;
          it != tmpmap.end() ;
          it++ ) 
    {
        insertGlobal(it->second);
    }

}


ContainerIdxSigEntry::ContainerIdxSigEntry()
    :preprocessed(false)
{
}

inline
void ContainerIdxSigEntry::preprocess()
{
    if (preprocessed == true)
        return;
    len = length.the_stack[0].init;
    stride = logical_offset.seq[0];
    num_chunks_per_seg = stride/len;
    bulk_size = stride * logical_offset.cnt;
    num_of_bulks = chunkmap.size() / num_chunks_per_seg;
    last_bulk_chunks = chunkmap.size() % num_chunks_per_seg;
    if ( last_bulk_chunks != 0 ) {
        num_of_bulks++;
    } else {
        last_bulk_chunks = num_chunks_per_seg;
    }

    map<pid_t, int> pidcnt;
    vector<SigChunkMap>::iterator it;
    for ( it = chunkmap.begin() ;
          it != chunkmap.end()  ;
          it++ )
    {
        if ( pidcnt.find( it->new_chunk_id ) == pidcnt.end() ) {
            pidcnt[it->new_chunk_id] = 0;
        } else {
            pidcnt[it->new_chunk_id]++;
        }
        it->physical_bulk_id = pidcnt[it->new_chunk_id];
        //mlog(IDX_WARN, "chunkmap: new_id: %d, physical_bulk_id: %d",
        //                it->new_chunk_id, it->physical_bulk_id );
    }

    preprocessed = true;
}


inline
bool ContainerIdxSigEntry::contains( const off_t &req_offset,
                                     off_t &o_offset,
                                     off_t &o_length,
                                     off_t &o_physical,
                                     off_t &o_new_chunk_id)
{
    //mlog(IDX_WARN, "In %s", __FUNCTION__);
    if ( chunkmap.size() == 1 ) {
        off_t delta_sum;
        
        if ( req_offset < logical_offset.init ) {
            //mlog(IDX_WARN, "offset < init");
            return false;
        }

       if (  logical_offset.seq.size() * logical_offset.cnt <= 1 
             || logical_offset.init == req_offset ) 
       {
            // Only one offset in logical_offset, just check that one
            // Note that 5, [2]^1 and 5, []^0 are the same, they represent only 5
            //mlog(IDX_WARN, "check the only one");
            o_length = length.the_stack[0].init;
            if ( isContain(req_offset, logical_offset.init, o_length) ) {
                o_offset = logical_offset.init;
                o_physical = physical_offset.getValByPos(0);
                o_new_chunk_id = chunkmap[0].new_chunk_id;
                return true;
            } else {
                return false;
            }
        }

        delta_sum = sumVector(logical_offset.seq);
        assert (delta_sum > 0); //let's not handl this at this time. TODO:
        
        off_t roffset = req_offset - logical_offset.init; //logical offset starts from init
        
        if ( roffset >= delta_sum * logical_offset.cnt )
            return false;
        
        off_t col = roffset % delta_sum; 
        off_t row = roffset / delta_sum; 

        if ( row >= logical_offset.cnt ) {
            int last_pos = logical_offset.cnt * logical_offset.seq.size() - 1;
            o_offset = logical_offset.getValByPos(last_pos);
            o_length = length.getValByPos(last_pos);
            if ( isContain(req_offset, o_offset, o_length) ) {
                o_physical = physical_offset.getValByPos(last_pos);
                o_new_chunk_id = chunkmap[0].new_chunk_id;
                return true;
            } else {
                return false;
            }
        } else {
            off_t sum = 0;
            int col_pos;

            for ( col_pos = 0 ; 
                  sum <= col ;
                  col_pos++ )
            {
                sum += logical_offset.seq[col_pos];
            }
            col_pos--;
            int pos = col_pos + row * logical_offset.seq.size();

            o_offset = logical_offset.getValByPos(pos);
            o_length = length.getValByPos(pos);
            if ( isContain(req_offset, o_offset, o_length ) ) {
                o_physical = physical_offset.getValByPos(pos);
                o_new_chunk_id = chunkmap[0].new_chunk_id;
                return true;
            } else {
                return false;
            }
        }
    } else {
        // Repeating cross-proc pattern
        if ( req_offset < logical_offset.init ) {
            //mlog(IDX_WARN, "offset < init");
            return false;
        }

        // Prepare useful values
        preprocess();
        if ( len == 0 ) {
            return false;
        }
        off_t roffset = req_offset - logical_offset.init; //logical offset starts from init

        // Find the right bulk
        off_t bulk_index = roffset / bulk_size;
        
        if ( bulk_index >= num_of_bulks ) {
            // out of the bulks
            return false;
        }

        int num_of_chunks_in_this_bulk;
        if ( bulk_index != num_of_bulks - 1 ) {
            // this is not the last bulk
            num_of_chunks_in_this_bulk = num_chunks_per_seg; 
        } else {
            // this is the last bulk
            num_of_chunks_in_this_bulk = last_bulk_chunks;
        }
        //mlog( IDX_WARN, "bulk_index: %lld, num_of_chunks_in_this_bulk: %d.", 
        //        bulk_index, num_of_chunks_in_this_bulk);

        // Find out the right segment
        off_t bulk_init = bulk_size * bulk_index;
        off_t bulk_roffset = roffset - bulk_init;
        off_t row = bulk_roffset / stride;
        off_t col_remaining = bulk_roffset % stride;
        //mlog( IDX_WARN, "row: %lld, col_remaining: %lld.", 
        //        row, col_remaining);


        // Find out the right chunk
        int chunk_index_in_bulk = col_remaining / len;
        if ( chunk_index_in_bulk >= num_of_chunks_in_this_bulk ) {
            return false;
        }
        //mlog( IDX_WARN, "chunk_index_in_bulk: %d.", chunk_index_in_bulk);

        o_offset = logical_offset.init // jump to init
                  + bulk_init         // jump to the right bulk
                  + row * stride      // jump to the right row (seg)
                  + chunk_index_in_bulk * len; // jump to the right chunk
        o_length = len;
        int chunkmap_index = bulk_index * num_of_chunks_in_this_bulk + chunk_index_in_bulk;
        o_physical = physical_offset.getValByPos(row) 
                    + physical_offset.the_stack[0].seq[0] 
                      * physical_offset.the_stack[0].cnt 
                      * chunkmap[chunkmap_index].physical_bulk_id; // jump to right bulk
        o_new_chunk_id = chunkmap[chunkmap_index].new_chunk_id;
        //mlog( IDX_WARN, "ooffset: %lld, olength: %lld, ophysical: %lld.",
        //                 o_offset, o_length, o_physical );
        return true;    
    }
}

ContainerIdxSigEntryList::ContainerIdxSigEntryList()
{
    last_hit = listmap.end();
}

bool ContainerIdxSigEntryList::lookup( const off_t &req_offset,
                             off_t &o_offset,
                             off_t &o_length,
                             off_t &o_physical,
                             off_t &o_new_chunk_id)
{
    if ( listmap.empty() ) {
        return false;
    }

    if ( last_hit != listmap.end() )
    {
        if ( last_hit->second.contains( 
                                    req_offset,
                                    o_offset,
                                    o_length,
                                    o_physical,
                                    o_new_chunk_id) )
        {
            //mlog(IDX_WARN, "Hit pattern bookmark OHYEAH ");
            return true;
        }           
    }

    map<off_t, ContainerIdxSigEntry>::iterator entry_to_chk;
    entry_to_chk = listmap.upper_bound( req_offset );
    //if ( entry_to_chk != listmap.end() ) {
    //    mlog(IDX_WARN, "first to check:%s", entry_to_chk->second.show().c_str());
    //}

    //mlog(IDX_WARN, "In ContainerIdxSigEntryList lookup. Lookup [%lld]", req_offset);
    if ( entry_to_chk == listmap.begin() ) {
        //mlog(IDX_WARN, "every one is bigger than req_off");
        last_hit = listmap.end();
        return false;
    } else {
        // check begin() to entry_to_chk
        //mlog(IDX_WARN, "starts to check something");
        // It it is here, it can not be begin(),
        // so entry_to_chk-- is safe
        do {
            entry_to_chk--;
            //mlog(IDX_WARN, "In Loop to check %s", 
            //        entry_to_chk->second.show().c_str() );

            if ( entry_to_chk->second.contains( 
                                        req_offset,
                                        o_offset,
                                        o_length,
                                        o_physical,
                                        o_new_chunk_id) )
            {
                last_hit = entry_to_chk;
                return true;
            }
        } while (entry_to_chk != listmap.begin());
        last_hit = listmap.end();
        return false;
    }
    
}






