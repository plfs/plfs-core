/*
 *  patternanalyzer.h .cpp implement a tree structure for storing data. 
 *  It is not used any more since the maintenace is hard and it is not fast.
 *  
 */

#ifndef __patternanalyzer_h__
#define __patternanalyzer_h__

#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <stdint.h>
#include <assert.h>

#include "idxanalyzer.h"

using namespace std;

namespace MultiLevel {

    #define LEAF   'L'
    #define INNER  'I'
    typedef int32_t header_t;

    template <class T>
    void printVector( vector<T> vec )
    {
        typename vector<T>::const_iterator iiter;
        
        cout << "printVector: ";
        for ( iiter = vec.begin() ;
              iiter != vec.end() ;
              iiter++ )
        {
            cout <<  *iiter << ",";
        }
        cout << endl;        
    }


    class LeafTuple {
        public:
            int leaf_index;
            off_t leaf_delta_sum;

            LeafTuple(int a, off_t c)
                :leaf_index(a),
                 leaf_delta_sum(c)
            {}
    };


    class PatternAbstract {
        /*
        virtual string show() = 0;
        virtual bool follows( PatternAbstract &other ) = 0;
        virtual string serialize() = 0;
        virtual string deSerialize() = 0;
        */
    };

    class ChunkMap {
        public:
            pid_t original_chunk_id;
            pid_t new_chunk_id;
            int cnt;
    };

    // It is a tree structure used to describe patterns
    class DeltaNode: public PatternAbstract {
        public:
            DeltaNode();
            ~DeltaNode();
            void init();

            int cnt; // how many times the node repeats
            vector<DeltaNode *> children;
            vector<off_t> elements;       // one of sizes of 
                                          // children or elements must be 0
            
            
            bool isLeaf() const;
            string show() const;
            void freeChildren();
            void pushElement( off_t elem );
            bool isPopSafe( int limit );
            void popDeltas( int n );
            void popChild();
            void pushChild( DeltaNode *newchild );
            void push( DeltaNode *newchild );
            void push( off_t newelem );
            int getNumOfDeltas() const;
            void assign( vector<DeltaNode *>::const_iterator first,
                         vector<DeltaNode *>::const_iterator last );
            void assign( vector<off_t>::const_iterator first,
                         vector<off_t>::const_iterator last );
            void pushCopy( DeltaNode *nd );
            void pushCopy( off_t elm );
            string serialize() const;
            void deSerialize( string buf );
            off_t recoverPos( const int pos );
            LeafTuple getLeafTupleByPos( const int pos );
            off_t getDeltaSumUtilPos( const int pos );
            off_t getDeltaSum();
            int getNumOfLeaves();
            void buildPatterns( const vector<off_t> & seq );
            void compressMe(int win_size);
            static void deleteIt( DeltaNode * nd ); 
            static void deleteIt( off_t anoff ) ;
            void compressMyInit(int win_size = 6);
            bool isRepeating() const;
            void flattenMe();
            void append( const DeltaNode *other );
            void compressDeltaChildren(int win_size);
            bool isCompressionRatioGood();
            void expandMe();
            void clear();
            DeltaNode &operator=(const DeltaNode &other);
            ChunkMap getChunkByPos( const int pos );
    };



    class PatternCombo {
        public:
            vector<ChunkMap> chunkmap;
            double begin_timestamp;
            double end_timestamp;
            DeltaNode logical_offset;
            DeltaNode length;
            DeltaNode physical_offset;
        
            void buildFromHostEntries( const vector<HostEntry> &entry_buf, 
                                       const pid_t &proc );
            ~PatternCombo();
            string show();
            string serialize();
            void deSerialize(string );
            void append( const PatternCombo &other );
            int getNumOfVal();
            bool getDetailsByLogicalOff( const off_t logical,
                                         off_t rlength,
                                         off_t rphysical_offset,
                                         pid_t rorigin_id );
            bool expandBadCompression();
            void saveToFile( const int fd );
            void clear();
            bool contains( const off_t logical,
                                 off_t &ological,
                                 off_t &olength,
                                 off_t &ophysical_offset,
                                 pid_t &newid);
            ChunkMap getChunkByPos( const int pos );
    };


    ///////////////////////////////////////////////////////////
    // Pattern Unit
    ///////////////////////////////////////////////////////////
    class PatternUnitAbstract: public PatternAbstract
    {
    };


    class PatternUnit: public PatternAbstract
    {
        public:
            PatternUnit()
                :cnt(0)
            {}
            vector<off_t> seq;
            int cnt;
            string show();
    };

    class PatternHeadUnit: public PatternUnit
    {
        public:
            PatternHeadUnit ()
                :init(0)
            {}
            PatternHeadUnit (off_t x);
            off_t init;
            string show();
    };

    class PatternInitUnit: public PatternUnitAbstract
    {
        public:
            off_t init;
    };

    ///////////////////////////////////////////////////////////

    class PatternBlock: public PatternAbstract 
    {
        public:
            vector<PatternHeadUnit> block;
            string show();
    };

    // A pattern chunk is a pattern with multi levels
    class PatternChunk: public PatternAbstract
    {
        public:
            vector<PatternBlock> chunk;
    };

    class PatternChunkList: public PatternAbstract
    {
        public:
            vector<PatternChunk> list; 
    };
    
    class PatternEntry: public PatternAbstract
    {
        public:
            pid_t original_chunk_id;
            pid_t new_chunk_id;
            PatternChunk logical_offset;
            PatternChunk length;
            PatternChunk physical_offset;
    };

    class PatternEntryList: public PatternAbstract
    {
        public:
            vector<PatternEntry> list;
    };

    class PatternAnalyzer 
    {
        public:
            PatternChunk generatePatterns( PatternBlock &pblock );
    };

    ////////////////////////////////////////////////////////
    //
    //
    ////////////////////////////////////////////////////////
    
    void appendToBuffer( string &to, const void *from, const int size );
    void readFromBuf( string &from, void *to, int &start, const int size );
    
    template <class TYPE>  // TYPE could be off_t or DeltaNode *
    class Tuple {
        public:
            int offset; //note that this is not the 
            // offset when accessing file. But
            // the offset in LZ77 algorithm
            int length; //concept in LZ77
            TYPE next_symbol;

            Tuple() {}
            Tuple(int o, int l, TYPE n); 

            void put(int o, int l, TYPE n);
            bool operator== (const Tuple other);
            // Tell if the repeating sequences are next to each other
            bool isRepeatingNeighbor();
            string show();
            string getSymbolStr(off_t sym);
            string getSymbolStr(DeltaNode *sym);
    };

    template <class TYPE>
    Tuple <TYPE> searchNeighbor( 
                    vector<TYPE> const &seq,
                    typename vector<TYPE>::const_iterator p_lookahead_win, 
                    int win_size ); 


    template <class TYPE>

    bool isEqual(DeltaNode* a, DeltaNode* b);
    bool isEqual(off_t a, off_t b);

    // Input:  vector<T> deltas
    // Output: DeltaNode describing patterns in deltas
    //
    // deltas is got by a sequence of inits. Later the output can be
    // used to combine inits with deltas
    // This function finds out the pattern by LZ77 modification
    template <class TYPE>
    DeltaNode* findPattern( vector<TYPE> const &deltas, int win_size )
    {
        typename vector<TYPE>::const_iterator lookahead_win_start; 
        DeltaNode *pattern_node = new DeltaNode; // pattern_node only plays with
                                                 // leaves in deltas, do not
                                                 // create new leaves
        pattern_node->cnt = 1; //remind you that this level does not repeat

        lookahead_win_start = deltas.begin();
        
        while ( lookahead_win_start != deltas.end() ) {
            //lookahead window is not empty
            Tuple<TYPE> cur_tuple = searchNeighbor( deltas, 
                                              lookahead_win_start, 
                                              win_size );
            //cout << "TUPLE:" << cur_tuple.show() << endl;
            if ( cur_tuple.isRepeatingNeighbor() ) {
                DeltaNode winnode;
                winnode.assign( lookahead_win_start,
                                lookahead_win_start + cur_tuple.length);
                 
                //cout << "--- in window length:" << winnode.getNumOfDeltas() << endl;
                int lookwin_delta_len = winnode.getNumOfDeltas();
                //cout << "--- " << winnode.show() << endl;


                if ( pattern_node->isPopSafe( lookwin_delta_len ) ) {
                    //safe
                    //cout << "--safe" << endl;
                    pattern_node->popDeltas( lookwin_delta_len );

                    typename vector<TYPE>::const_iterator first, last;
                    first = lookahead_win_start;
                    last = lookahead_win_start + cur_tuple.length;

                    DeltaNode *combo_node = new DeltaNode;  
                    combo_node->assign(first, last);
                    combo_node->cnt = 2;

                    pattern_node->pushChild( combo_node  );
                    lookahead_win_start += cur_tuple.length;
                } else {
                    //unsafe
                    //cout << "--unsafe" << endl;
                    assert(pattern_node->children.size() > 0); // window moved,
                                                               // so some patterns must be in children
                    DeltaNode *lastchild = pattern_node->children.back();
                    
                    // check if the last child(pattern) can be used to 
                    // represent the repeating neighbors
                    int lastchild_pattern_length = 0; 
                    lastchild_pattern_length = lastchild->getNumOfDeltas()/lastchild->cnt;
                    //cout << "--- lastchild_pattern_length:" << lastchild_pattern_length << endl;
                    
                    if ( lookwin_delta_len % lastchild_pattern_length == 0
                         && lookwin_delta_len <= lastchild_pattern_length * lastchild->cnt ) 
                    { //TODO: need to justify this
                        //the subdeltas in lookahead window repeats
                        //the last pattern in pattern_node
                        lastchild->cnt +=  lookwin_delta_len/lastchild_pattern_length;
                        //cout << "---- repeats last pattern. add " 
                        //     << (lookwin_delta_len/lastchild_pattern_length) << "to last pattern" << endl;

                        lookahead_win_start += cur_tuple.length;
                    } else {
                        //cannot pop out cur_tuple.length elems without
                        //totally breaking any pattern.
                        //So we simply add one elem to the stack
                        //cout << "---- cannot pop out. add new" << endl;
                        DeltaNode *newchild = new DeltaNode;
                        newchild->push( *lookahead_win_start );
                        newchild->cnt = 1;

                        pattern_node->pushChild(newchild);
                        
                        lookahead_win_start++;
                    }
                }
            } else {
                //(0,0,x)
                //cout << "-no repeating neighor" << endl;
                DeltaNode *newchild = new DeltaNode;
                newchild->push( cur_tuple.next_symbol );
                newchild->cnt = 1;
                pattern_node->pushChild(newchild);
                
                lookahead_win_start++;
            }
            
            //compress repeats in last child
            assert( pattern_node->children.size() > 0 );
            DeltaNode *lastchild = pattern_node->children.back();

            if ( lastchild->isRepeating() ) {
                if ( lastchild->isLeaf() ) {
                    lastchild->cnt = lastchild->getNumOfDeltas();
                    lastchild->elements.erase(
                            lastchild->elements.begin()+1,
                            lastchild->elements.end() ); //keep the first one
                    assert( lastchild->elements.size() == 1 );
                } else {
                    lastchild->cnt = 
                            lastchild->children.size() * lastchild->cnt;
                    lastchild->children.erase(
                            lastchild->children.begin() + 1,
                            lastchild->children.end() );
                    assert( lastchild->children.size() == 1 );
                }
            }

            //cout << pattern_node->show() << endl;
        }
     
        // Now pattern_node's children are all new allocated in this
        // function. We clean it up here after we make a copy of all stuff
        // in pattern_node

        // copying
        DeltaNode *pattern_node_copy = new DeltaNode;
        pattern_node_copy->deSerialize( pattern_node->serialize() );
    

        // free the space
        vector<DeltaNode * >::iterator pit;
        for ( pit =  pattern_node->children.begin() ;
              pit != pattern_node->children.end() ;
              pit++ )
        {
            DeltaNode::deleteIt( *pit );
        }
        delete pattern_node;

        //flatten it to save space and possibly time
        // Don't flatten pattern_node_copy,
        // otherwise it may become a leaf!
        for ( pit = pattern_node_copy->children.begin() ;
              pit != pattern_node_copy->children.end() ;
              pit++ )
        {
            (*pit)->flattenMe();
        }
        
        return pattern_node_copy;
    }

    ////////////////////////////////////////////////////////////////
    //  Tuple : the concept in LZ77
    ////////////////////////////////////////////////////////////////
    template <class TYPE>
    Tuple<TYPE>::Tuple(int o, int l, TYPE n) {
        offset = o;
        length = l;
        next_symbol = n;
    }
    
    template <class TYPE>
    void Tuple<TYPE>::put(int o, int l, TYPE n) {
        offset = o;
        length = l;
        next_symbol = n;
    }

    template <class TYPE>
    bool Tuple<TYPE>::operator== (const Tuple other) {
        if (offset == other.offset 
                && length == other.length
                && isEqual( next_symbol == other.next_symbol ) )
        {
            return true;
        } else {
            return false;
        }
    }

    template <class TYPE>
    bool Tuple<TYPE>::isRepeatingNeighbor() {
        return (offset == length && offset > 0);
    }
    
    template <class TYPE>
    string Tuple<TYPE>::show() {
        ostringstream showstr;
        showstr << "(" << offset 
            << ", " << length
            << ", " << getSymbolStr(next_symbol) 
            << ")" << endl;
        return showstr.str();
    }

    template <class TYPE>
    string Tuple<TYPE>::getSymbolStr(off_t sym) 
    {
        ostringstream oss;
        oss << sym;
        return oss.str();
    }

    template <class TYPE>
    string Tuple<TYPE>::getSymbolStr(DeltaNode *sym) 
    {
        ostringstream oss;
        oss << sym->show();
        return oss.str();
    }




    template <class TYPE>
    Tuple <TYPE> searchNeighbor( 
                    vector<TYPE> const &seq,
                    typename vector<TYPE>::const_iterator p_lookahead_win, 
                    int win_size ) 
    {
        typename vector<TYPE>::const_iterator i;     
        int j;

        //i goes left util the begin or reaching window size
        i = p_lookahead_win;
        int remain = seq.end() - p_lookahead_win;
        while ( i != seq.begin() 
                && (p_lookahead_win - i) < win_size
                && (p_lookahead_win - i) < remain ) {
            i--;
        }

        //i points to a element in search buffer where matching starts
        //j is the iterator from the start to the end of search buffer to compare
        for ( ; i != p_lookahead_win ; i++ ) {
            int search_length = p_lookahead_win - i;
            for ( j = 0 ; j < search_length ; j++ ) {
                if ( !isEqual( *(i+j), *(p_lookahead_win + j)) ) { //TODO: need to impliment comparison
                    break;
                }
            }
            if ( j == search_length ) {
                //found a repeating neighbor
                return Tuple<TYPE>(search_length, search_length, 
                        *(p_lookahead_win + search_length));
            }
        }

        //Cannot find a repeating neighbor
        return Tuple<TYPE>(0, 0, *(p_lookahead_win));
    }


}

#endif
