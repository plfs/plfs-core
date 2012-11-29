/*
 *  patternanalyzer.h .cpp implement a tree structure for storing data. 
 *  It is not used any more since the maintenace is hard and it is not fast.
 */


#include "patternanalyzer.h"
#include "Util.h"

#include <string.h>
#include <stack>
#include <algorithm>

namespace MultiLevel {
    ////////////////////////////////////////////////////////////////
    //  MISC
    ////////////////////////////////////////////////////////////////

    inline bool isContain( off_t off, off_t offset, off_t length )
    {
        //ostringstream oss;
        //oss << "isContain(" << off << ", " << offset << ", " << length << ")" <<  endl;
        //mlog(IDX_WARN, "%s", oss.str().c_str());
        return ( offset <= off && off < offset+length );
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

    vector<off_t> buildDeltas( vector<off_t> seq ) 
    {
        vector<off_t>::iterator it;
        vector<off_t> deltas;
        for ( it = seq.begin() ; it != seq.end() ; it++ )
        {
            if ( it > seq.begin() ) {
                deltas.push_back( *it - *(it-1) );
            }
        }
        //cout << "in builddeltas: " << seq.size() << " " << deltas.size() << endl; 
        return deltas;
    }

    
    PatternBlock buildDeltaBlock( PatternBlock &pblock )
    {
        vector<PatternHeadUnit>::iterator it;
        PatternBlock deltas;
        for ( it = pblock.block.begin();
                it != pblock.block.end();
                it++ ) 
        {
            if ( it != pblock.block.begin() ) {
                PatternHeadUnit phu;
                phu.init = it->init - (it-1)->init;
                deltas.block.push_back( phu );
            }
        }
        return deltas;
    }
    
    bool isEqual(DeltaNode* a, DeltaNode* b)
    {
        //TODO: implement this
        return a->serialize() == b->serialize();
    }

    bool isEqual(off_t a, off_t b)
    {
        return a==b;
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


    ////////////////////////////////////////////////////////////////
    //  DeltaNode
    ////////////////////////////////////////////////////////////////
    bool DeltaNode::isLeaf() const
    {
        // if there's no children, then this node is a leaf
        return children.size() == 0 ;
    }

    string DeltaNode::show() const
    {
        ostringstream oss;

        if ( this->isLeaf() ) {
            oss << "(" ;
            vector<off_t>::const_iterator it;
            for ( it = elements.begin() ;
                  it != elements.end()  ;
                  it++ )
            {
                oss << *it;
                if ( it+1 != elements.end() ) {
                    oss << ",";
                }
            }
            oss << ")" ;
            oss << "^" << cnt;
        } else {
            // Not a leaf, recurse
            oss << "[";
            vector<DeltaNode*>::const_iterator it;
            for ( it =  children.begin() ;
                  it != children.end()   ;
                  it++ )
            {
                oss << (*it)->show();
                if ( it+1 != children.end() ) {
                    oss << ";";
                }
            }
            oss << "]";
            oss << "^" << cnt;
        }

        return oss.str();
    }

    // Delete all children of this node
    // This makes this node become a leaf
    void DeltaNode::freeChildren() 
    {
        vector<DeltaNode*>::const_iterator it;
        while ( !children.empty() )
        {
            DeltaNode *pchild = children.back();
            pchild->freeChildren();
            delete pchild;
            children.pop_back();
        }
        return;
    }

    // Caller to make sure it is a leaf
    void DeltaNode::pushElement( off_t elem )
    {
        assert( isLeaf() ); // only leaf can have elements
        elements.push_back(elem);
    }

    bool DeltaNode::isPopSafe( int limit )
    {
        // limit is the number of deltas to pop out
        assert( !this->isLeaf() ); //assume this is not a leaf
                                   //since it is used for storing patterns
        vector<DeltaNode *>::reverse_iterator rit;
        int total = 0;

        rit = children.rbegin();
        while ( rit != children.rend() && total < limit ) 
        {
            total += (*rit)->getNumOfDeltas();
            rit++;
        }
        //cout << "in " << __FUNCTION__ 
        //    << " total: " << total
        //    << " limit: " << limit << endl;
        return total == limit; //exactly limit of deltas can be poped out
    }

    // To get how many deltas is in this node
    // if all patterns in this node is expanded
    int DeltaNode::getNumOfDeltas() const
    {
        
        if ( this->isLeaf() ) {
            int total = 0;
            total = elements.size() * cnt;
            return total;
        } else {
            int total = 0;
            vector<DeltaNode *>::const_iterator it;
            
            for ( it =  children.begin() ;
                  it != children.end() ;
                  it++ )
            {
                total += (*it)->getNumOfDeltas();
            }
            total *= cnt; // the whole pattern may repeat
            return total;
        }
    }



    // Only used by popDeltas
    void DeltaNode::deleteIt( DeltaNode * nd ) 
    {
        delete nd;
    }
    
    void DeltaNode::deleteIt( off_t anoff ) 
    {
        return ;
    }

    // pop out n expanded deltas out
    // Caller has to make sure it is safe by calling isPopSafe()
    // THIS JUST FREES THE ONE LOWER LEVEL OF THE TREE
    void DeltaNode::popDeltas( int n )
    {
        assert( isPopSafe(n) );
        
        int poped = 0;
        while ( poped < n ) {
            DeltaNode *ptopop = children.back();
            poped += ptopop->getNumOfDeltas();
            deleteIt(poped);
            children.pop_back();
        }
    }

    // pop out the last child in children[]
    // free its space
    void DeltaNode::popChild()
    {
        if ( children.size() > 0 ) {
            DeltaNode *pchild = children.back();
            pchild->freeChildren();
            delete pchild;
            children.pop_back();
        }
        return;
    }

    // pushChild will make a node NOT a leaf
    // If this node is an inner node, calling this is safe
    // If this node is a leaf:
    //      If it has elements: NOT SAFE
    //      If it has no elements: safe. And this function will
    //                             make this node a inner node.
    void DeltaNode::pushChild( DeltaNode *newchild )
    {
        assert( elements.size() == 0 );
        children.push_back(newchild);
    }

    void DeltaNode::assign( vector<DeltaNode *>::const_iterator first,
                 vector<DeltaNode *>::const_iterator last )
    {
        children.assign( first, last );
    }

    void DeltaNode::assign( vector<off_t>::const_iterator first,
                 vector<off_t>::const_iterator last )
    {
        elements.assign( first, last );
    }

    void DeltaNode::push( DeltaNode *newchild ) {
        pushChild(newchild);
    }
    
    void DeltaNode::push( off_t newelem ) 
    {
        pushElement(newelem);
    }

    void DeltaNode::pushCopy( DeltaNode *nd )
    {
        //cout << "Start pushCopy" << endl;
        string buf = nd->serialize();
        //cout << nd->show() << endl;
        //cout << "Serialized" << endl;
        DeltaNode *newone = new DeltaNode;;
        newone->deSerialize(buf);
        //cout << "deSerialized" << endl;
        //cout << "newone:" << newone->show() << endl;
        pushChild(newone);
        //cout << "End pushCopy" << endl;
    }

    void DeltaNode::pushCopy( off_t elm )
    {
        push(elm);
    }

    string DeltaNode::serialize() const
    {
        string buf;
        //cout << "++++++Start Serialzing " << this->show() << endl;
        if ( isLeaf() ) {
            //[L][REP][Pattern Size][elements]
            char buftype = LEAF; //L means leaf
            appendToBuffer(buf, &buftype, sizeof(buftype));
            appendToBuffer(buf, &cnt, sizeof(cnt));
            header_t elembodysize = elements.size() * sizeof(off_t); 
            appendToBuffer(buf, &elembodysize, sizeof(elembodysize));
            if ( elembodysize > 0 ) {
                appendToBuffer(buf, &elements[0], elembodysize);
            }
            //cout << "TYPE:[" << buftype << "] bodysize:" << elembodysize << endl;
            //cout << "++++++End   Serialzing " << this->show() << endl;
            return buf;
        } else {
            //[I][REP][Pattern Size][ [..] [...] ..  ]
            char buftype = INNER; // I means inner
            appendToBuffer(buf, &buftype, sizeof(buftype));
            appendToBuffer(buf, &cnt, sizeof(cnt));
            header_t bodysize = 0; //wait to be updated
            int bodysize_pos = buf.size(); // buf[bodysize_pos] is where
                                           // the bodysize is
            appendToBuffer(buf, &bodysize, sizeof(bodysize));
            
            vector<DeltaNode *>::const_iterator it;
            for ( it =  children.begin() ;
                  it != children.end()   ;
                  it++ )
            {
                string tmpbuf;
                tmpbuf = (*it)->serialize();
                appendToBuffer(buf, tmpbuf.c_str(), tmpbuf.size());
            }
            header_t *psize = (header_t *) &buf[bodysize_pos]; 
            *psize = buf.size() 
                     - (sizeof(buftype) + sizeof(cnt) + sizeof(bodysize));
            //cout << "TYPE:[" << buftype << "] bodysize:" << *psize
            //     << "bodysize_pos" << bodysize_pos
            //     << "buf.size():" << buf.size() << endl;
            //cout << "++++++End   Serialzing " << this->show() << endl;
            return buf;
        }
    }

    void DeltaNode::deSerialize( string buf )
    {
        char buftype;
        header_t bodysize = 0;
        int cur_start = 0;

        //cout << "***** START deSerialize " << endl;

        readFromBuf( buf, &buftype, cur_start, sizeof(buftype));
        readFromBuf( buf, &cnt, cur_start, sizeof(cnt));
        readFromBuf( buf, &bodysize, cur_start, sizeof(bodysize));
      
        //cout << "**TYPE:" << buftype << endl;
        //cout << "cnt:" << cnt << endl;
        //cout << "bodysize:" << bodysize << endl;
        //cout << "bufsize:" << buf.size() << endl;
        
        if ( buftype == LEAF ) {
            //only elements left in buf
            //cout << "--type is leaf" << endl;
            if ( bodysize > 0 ) {
                //cout << "cur_start :" << cur_start << endl;
                elements.resize( bodysize/sizeof(off_t) );
                readFromBuf( buf, &elements[0], cur_start, bodysize);
            }
        } else {
            // It is a inner node in buf
            //cout << "--type is inner" << endl;
            while ( cur_start < buf.size() ) {
                //cout << "----STARTLOOP----- cur_start:" << cur_start
                //     << " buf.size():" << buf.size() << endl;
                char btype;
                int bcnt;
                header_t bbodysize;
                int bufsize;

                readFromBuf( buf, &btype, cur_start, sizeof(btype));
                readFromBuf( buf, &bcnt, cur_start, sizeof(bcnt));
                readFromBuf( buf, &bbodysize, cur_start, sizeof(bbodysize));



                bufsize = sizeof(btype) + sizeof(bcnt) 
                          + sizeof(bbodysize) + bbodysize;

                //cout << "--------- TYPE:" << btype << " CNT:" << cnt
                //     << " bodysize:" << bbodysize 
                //     << " bufsize:" << bufsize << endl;
                string localbuf;
                localbuf.resize( bufsize );
                cur_start -= (sizeof(btype) + sizeof(bcnt)
                              + sizeof(bbodysize));
                             
                readFromBuf( buf, &localbuf[0], cur_start, bufsize);

                DeltaNode *newchild = new DeltaNode;
                newchild->deSerialize(localbuf);

                pushChild(newchild);
                //cout << "----ENDLOOP----- cur_start:" << cur_start
                //     << " buf.size():" << buf.size() << endl;
            }
        }
        //cout << this->show() << endl;
        //cout << "***** END   deSerialize " << endl;
    }

    void DeltaNode::init()
    {
        cnt = 1;
    }

    DeltaNode::DeltaNode()
    {
        init();
    }

    // this constructor turns this DeltaNode into
    // a pattern for logical_offset, length, or physical_offset
    // The format is:
    // [init...][Delta...] 
    //                    [Delta...] 
    //                                [Delta...] ...
    // [] is a child. [init] is the only place that can be splitted.
    void DeltaNode::buildPatterns( const vector<off_t> &seq )
    {
        //cout << "in " << __FUNCTION__ << endl;
        init();
       
        if ( seq.size() == 0 ) {
            // no input, do nothing
            return ;
        }

        elements = seq;
        compressMyInit(6);
        
        // compress the second time
        //children[1]->compressMe(20);
        //compressMyInit(6);

        // you can compress more by calling
        // compressMyInit()
        // or to compress deltas, call delta->compressMe for them
       
        /*
        // compress deltas
        vector<DeltaNode *>::const_iterator it;
        for ( it =  children.begin() ;
              it != children.end() ;
              it++ )
        {
            if ( it != children.begin() ) {
               (*it)->compressMe(10); 
            }
        }
        */

        return;
    }

    void DeltaNode::compressDeltaChildren(int win_size) {
        vector<DeltaNode *>::iterator it;
        for ( it =  children.begin();
              it != children.end() ;
              it++ ) 
        {
            if ( it != children.begin() ) {
                (*it)->compressMe(win_size);
            }
        }
        return;
    }


    // It compresses [init..] in this [init..][delta...][delta..]
    // [init..] is splitted to [new init..][new delta..]
    void DeltaNode::compressMyInit(int win_size)
    {
        if ( isLeaf() ) {
            // this: [init.. ]^1
            // after: [[init..][delta..]..]

            vector<off_t> deltas;
            DeltaNode *deltas_pattern;
            DeltaNode *inits = new DeltaNode;
            
            deltas = buildDeltas( elements );

            deltas_pattern = findPattern( deltas, win_size );

            // handle the inits
            if ( elements.size() == 0 ) {
                // nothing can be done
                return; 
            }
            int pos = 0;
            inits->pushElement( elements[pos] );
            vector<DeltaNode *>::const_iterator it;
            for ( it =  deltas_pattern->children.begin() ;
                  it != deltas_pattern->children.end() ;
                  it++ )
            {
                pos += (*it)->getNumOfDeltas();
                //cout << pos << "<" << seq.size() << endl;
                assert( pos < elements.size() );
                inits->pushElement( elements[pos] );
            }
            elements.clear();
            pushChild(inits);
            pushChild(deltas_pattern);
            return;           
        } else {
            // not a leaf
            // this: [[init...][delta.A.][delta.B..]..]
            // after:[[init new][delta new..] [delta.A.][delta.B..]..]

            assert( this->children[0]->isLeaf() );
            
            this->children[0]->compressMyInit();
            // this: [ [[init...][delta..]]  [delta.A.][delta.B..]..]

            DeltaNode *compressedInit = this->children[0];

            //move children of compressedInit up one level
            this->children.erase( this->children.begin() );

            vector<DeltaNode *>::const_reverse_iterator rit;
            for ( rit =  compressedInit->children.rbegin() ;
                  rit != compressedInit->children.rend() ;
                  rit++ )
            {
                this->children.insert( this->children.begin(), *rit );
            }
            return;
        }
    }

    void DeltaNode::clear()
    {
        elements.clear();
        freeChildren();
    }

    // It tries to use LZ77 to find repeating pattern in
    // the children
    void DeltaNode::compressMe(int win_size)
    {
       
        if ( isLeaf() ) {
            DeltaNode *compressedChild 
                       = findPattern( this->elements, win_size );
            this->elements.clear(); 
            children = compressedChild->children;
        } else {
            // Not a leaf
            DeltaNode *compressedChild 
                       = findPattern( this->children, win_size );
            freeChildren();
            children = compressedChild->children;
        }
    }

    // input is the pos of a delta
    // leaf_pos is the output, it is the position of the leaf
    // it returns the delta sun util pos in the leaf
    LeafTuple DeltaNode::getLeafTupleByPos( const int pos )
    {
        LeafTuple tup(0,0);
        int rpos = pos;
        //cout << __FUNCTION__ << ":" << pos
        //     << "NumOfDeltas:" << this->getNumOfDeltas() << endl;
        assert( rpos <= this->getNumOfDeltas() );

        if ( isLeaf() ) {
            // hit what you want
            if ( rpos == 0 ) {
                tup.leaf_delta_sum = 0;
            } else {
                tup.leaf_delta_sum = this->getDeltaSumUtilPos(rpos - 1);
            }
            tup.leaf_index = 0; // tup.leaf_index can be used as an index
                                // leaves[ leaf_index ] is this one
            return tup;
        } else {
            // [..][..]...
            int num_child_deltas = this->getNumOfDeltas()/cnt;
            
            int num_child_leaves = this->getNumOfLeaves()/cnt;

            assert( num_child_deltas > 0 );
            
            int col = rpos % num_child_deltas;
            int row = rpos / num_child_deltas;

            if ( row > 0 ) {
                tup.leaf_index = (this->getNumOfLeaves()/cnt) * row;
            }

            vector<DeltaNode *>::const_iterator it;
            for ( it = children.begin() ;
                  it != children.end()  ;
                  it++ )
            {
                int sizeofchild = (*it)->getNumOfDeltas();
                if ( col < sizeofchild ) {
                    LeafTuple ltup = (*it)->getLeafTupleByPos( col );
                    tup.leaf_index += ltup.leaf_index;
                    tup.leaf_delta_sum = ltup.leaf_delta_sum;
                    break;
                } else {
                    tup.leaf_index += (*it)->getNumOfLeaves();

                    col -= sizeofchild;
                }          
            }
            return tup;
        }
    }

    int DeltaNode::getNumOfLeaves()
    {
        int sumleaves = 0;

        if ( isLeaf() ) {
            return 1;
        } else {
            vector<DeltaNode *>::const_iterator it;
            
            for ( it = children.begin() ;
                  it != children.end()  ;
                  it++ )
            {
                sumleaves += (*it)->getNumOfLeaves();
            }

            return sumleaves * cnt;
        }
    }

    off_t DeltaNode::getDeltaSumUtilPos( const int pos )
    {
        int rpos = pos;
        assert( rpos < this->getNumOfDeltas() );

        if ( isLeaf() ) {
            off_t delta_sum = 0;
            int col = rpos % elements.size();
            int row = rpos / elements.size();

            assert ( row < cnt );
            off_t elem_sum = sumVector(elements);

            //+1 so include the one pointed by col
            off_t last_row_sum 
                  = sumVector( vector<off_t> (elements.begin(),
                                              elements.begin() + col + 1) ); 
            return elem_sum * row + last_row_sum; 
        } else {
            off_t sum = 0;
            int num_child_deltas = this->getNumOfDeltas()/cnt;
            assert( num_child_deltas > 0 );
            int col = rpos % num_child_deltas;
            int row = rpos / num_child_deltas;

            if ( row > 0 ) {
                off_t childsum = this->getDeltaSum() / cnt; //TODO: justify this
                /*
                vector<DeltaNode *>::const_iterator cit;
                for ( cit = children.begin() ;
                      cit != children.end() ;
                      cit++ )
                {
                    childsum += (*cit)->getDeltaSum();
                }
                */
                sum += childsum * row;
            }

            vector<DeltaNode *>::const_iterator it;
            for ( it = children.begin() ;
                  it != children.end()  ;
                  it++ )
            {
                int sizeofchild = (*it)->getNumOfDeltas();
                if ( col < sizeofchild ) {
                    sum += (*it)->getDeltaSumUtilPos( col );
                    break;
                } else {
                    sum += (*it)->getDeltaSum();
                    col -= sizeofchild;
                }          
            }
            return sum;
        }
    }

    off_t DeltaNode::getDeltaSum()
    {
        if ( isLeaf() ) {
            return sumVector(elements)*cnt;
        } else {
            off_t sum = 0;
            vector<DeltaNode *>::const_iterator it;
            for ( it =  children.begin() ;
                  it != children.end()   ;
                  it++ )
            {
                sum += (*it)->getDeltaSum();
            }
            return sum*cnt;
        }
    }

    // this DeltaNode must have format: [init...][delta...] [delta...]...[last]
    off_t DeltaNode::recoverPos( const int pos )
    {
        // find the ID of leaf who have the pos'th delta in it in last
        // pos = id
        // last--

        vector<off_t> deltalist;
        int leafpos = 0;
        int rpos = pos;
        vector<DeltaNode *>::const_reverse_iterator rit;

        for ( rit =  children.rbegin() ;
              rit != children.rend()   ;
              rit++ )
        {
            //cout << "------------ a loop ------------" <<endl;
            if ( rit + 1 == children.rend() ) {
                // this is the DeltaNode of [init...]
                assert( (*rit)->isLeaf() );
                assert( rpos < (*rit)->elements.size() );
                assert( (*rit)->cnt == 1 );
                off_t deltasum = sumVector( deltalist );
                //cout << "in [init..]" << "deltasum:" << deltasum
                //     << "rpos: " << rpos
                //     << "elem[rpos]: " << (*rit)->elements[rpos] << endl;
                return (*rit)->elements[rpos] + deltasum;
            } else {
                // pure delta node
                LeafTuple tup = (*rit)->getLeafTupleByPos(rpos);
                deltalist.push_back( tup.leaf_delta_sum );
                rpos = tup.leaf_index;
                //cout << "tup.leaf_index: " << tup.leaf_index << endl;
                //cout << "delta list:" ;
                //cout << endl;
            }
        }
        assert( 0 );
    }

    DeltaNode::~DeltaNode() 
    {
        //freeChildren(); let the creator decide when to delete
    }

    bool DeltaNode::isRepeating() const 
    {
        if ( isLeaf() ) {
            vector<off_t>::const_iterator it;
            for ( it =  elements.begin() ;
                  it != elements.end() ;
                  it++ )
            {
                if ( it != elements.begin() 
                     && ( *it != *(it-1) ) ) 
                {
                    break;
                }
            }
            return it == elements.end(); // empty is also repeating
        } else {
            vector<DeltaNode *>::const_iterator it;
            for ( it =  children.begin() ;
                  it != children.end() ;
                  it++ )
            {
                if ( it != children.begin() 
                     && ( !isEqual(*it, *(it-1)) ) ) 
                {
                    break;
                }
            }
            return it == children.end(); // empty is also repeating
        }
    }

    void DeltaNode::flattenMe()
    {
        if ( isLeaf() ) {
            // cannot be flatter
            return;
        } else {
            //if I have only one child,
            // kill myself by becoming the child
            
            // Flat util cannot do so
            while ( children.size() == 1 ) {
                DeltaNode *pchild =  children[0];
                if ( pchild->isLeaf() ) {
                    assert ( elements.size() == 0 );
                    elements = pchild->elements;
                    cnt *= pchild->cnt;
                    freeChildren();
                    // Now I am a leaf
                } else {
                    //child has children
                    children = pchild->children;
                    cnt *= pchild->cnt;
                    delete pchild; // you don't want to destroy
                                   // whole child tree, since now
                                   // this node link to some children of children


                }
            }
            
            // Now this becomes a leaf or it has many children
            // recursive
            vector<DeltaNode *>::iterator it;
            for ( it = children.begin() ;
                  it != children.end() ;
                  it++ )
            {
                (*it)->flattenMe();
            }
        }
    }

    // It appends other to this.
    // other and this must have:
    //          1. cnt == 1
    //          2. same number of children
    // this: [ [ A ] [ B ] [ C ] ...]^1
    // other: [ [ 1 ] [ 2 ] [ 3 ] ...]^1
    // After:
    //       [ [ A 1 ] [ B 2 ] [ C 3 ] ... ]^1
    // if this and other are leaves
    // this: [ A B C ]^1
    // other:[ 1 2 3 ]^1
    // After:
    //       [ A B C 1 2 3 ]^1
    void DeltaNode::append( const DeltaNode *other )
    {
        //cout << "START append" << endl;
        //cout << "THIS: " << this->show() << endl;
        //cout << "OTHER:" << other->show() << endl;
        
        assert ( this->cnt == other->cnt && this->cnt == 1 );

        if ( other->isLeaf() ) {
            // This should be a leaf or empty
            // cout << "--- Other is leaf" << endl;
            assert( this->isLeaf() );
            elements.insert( elements.end(),
                             other->elements.begin(),
                             other->elements.end() );
        } else {
            // This should be empty or has same num
            // of children as other.
            // cannot be leaf
            //cout << "--- Other is NOT leaf. other.children.size() " 
            //     <<  other->children.size()  << endl;
            assert( (this->elements.empty() && this->children.empty())
                    || this->children.size() == other->children.size() );
            
            vector<DeltaNode *>::const_iterator oit, tit;
            
            if ( this->children.empty() ) {
                // copy all children of other in
                for ( oit = other->children.begin() ;
                      oit != other->children.end() ;
                      oit++ )
                {
                    this->pushCopy( *oit );
                }
            } else {
                for ( oit = other->children.begin(),
                      tit = this->children.begin() ;
                      oit != other->children.end(),
                      tit != this->children.end() ;
                      oit++, tit++ )
                {
                    assert( (*oit)->cnt == 1 );
                    assert( (*tit)->cnt == 1 );

                    if ( oit == other->children.begin() ) {
                        assert( (*tit)->isLeaf() );
                        (*tit)->elements.insert( 
                                (*tit)->elements.end(),
                                (*oit)->elements.begin(),
                                (*oit)->elements.end() );
                    } else {
                        DeltaNode padding;
                        padding.pushElement(0);
                        padding.cnt = 1;
                        (*tit)->pushCopy( &padding );
                        
                        assert( !(*tit)->isLeaf() );
                        vector<DeltaNode *>::const_iterator ooit;
                        for ( ooit = (*oit)->children.begin() ;
                              ooit != (*oit)->children.end() ;
                              ooit++ )
                        {
                            (*tit)->pushCopy( *ooit );
                        }
                    }
                }
            }
        }

        return;
    }

    
    bool DeltaNode::isCompressionRatioGood()
    {
        if ( isLeaf() ) {
            // [ x x x ] ^y are allways good
            return true; 
        } else {
            return children.front()->getNumOfDeltas() 
                   < (children.back()->getNumOfDeltas() / 2);
        }
    }


    DeltaNode &DeltaNode::operator=(const DeltaNode &other)
    {
        if ( this == &other )
            return *this;
        clear();
        this->deSerialize( other.serialize() );
    }

    // This node must not be a leaf
    // expand me from a tree to totally flat sequence of number, just
    // like: [1 2 3 4 5] ^1
    // after expanding, this becomes a tree with one fat leaf
    void DeltaNode::expandMe()
    {
        if ( isLeaf() ) {
            // Leaf cannot be expanded
            return;
        } else {
            int valsize = children.back()->getNumOfDeltas() + 1 ;
            int i;
            DeltaNode *newchild = new DeltaNode;

            for ( i = 0 ; i < valsize ; i++ ) {
                newchild->pushElement( recoverPos(i) );            
            }
            freeChildren();
            pushChild(newchild);

            /*
            assert( elements.empty() ); 
            stack<DeltaNode *> ndstack;

            ndstack.push( this );
            while ( !ndstack.empty() ) {
                DeltaNode *nd = ndstack.top();
                ndstack.pop();

                if ( nd->isLeaf() ) {
                    int icnt;
                    for ( icnt = 0 ; icnt < nd->cnt ; icnt++ ) {
                        vector<off_t>::const_iterator eit;
                        for ( eit = nd->elements.begin();
                              eit != nd->elements.end();
                              eit++ )
                        {
                            if ( elements.empty() ) {
                                this->elements.push_back(*eit);
                            } else {
                                this->elements.push_back( )
                                        
                            }
                        }                        
                    }
                } else {
                    vector<DeltaNode *>::const_reverse_iterator rit;
                    for ( rit = nd->children.rbegin() ;
                          rit != nd->children.rend() ;
                          rit++ ) {
                        ndstack.push(*rit);
                    }
                }
            }
            */
        }
    }

    ////////////////////////////////////////////////////////////////
    //  PatternCombo
    ////////////////////////////////////////////////////////////////
    void PatternCombo::buildFromHostEntries( const vector<HostEntry> &entry_buf, 
                                             const pid_t &proc )
    {
        vector<off_t> h_logical_off, h_length, h_physical_off; 
        vector<HostEntry>::const_iterator iter;
        
        for ( iter = entry_buf.begin() ; 
                iter != entry_buf.end() ;
                iter++ )
        {
            if ( iter->id != proc ) {
                continue;
            }

            h_logical_off.push_back(iter->logical_offset);
            h_length.push_back(iter->length);
            h_physical_off.push_back(iter->physical_offset);
        }
        
        this->logical_offset.buildPatterns( h_logical_off );
        this->length.buildPatterns( h_length );
        this->physical_offset.buildPatterns( h_physical_off );
        
        if ( !this->chunkmap.empty() &&
             this->chunkmap.back().original_chunk_id == proc ) {
            this->chunkmap.back().cnt += h_logical_off.size();
        } else {
            ChunkMap map;
            map.original_chunk_id = proc;
            map.cnt = h_logical_off.size();
            this->chunkmap.push_back( map );
        }
    }

    string PatternCombo::show()
    {
        ostringstream oss;
        vector<ChunkMap>::const_iterator it;
        for ( it = chunkmap.begin() ;
              it != chunkmap.end() ;
              it++ )
        {
            oss << "[" << it->original_chunk_id 
                << ", " << it->new_chunk_id 
                << ", " << it->cnt << "]";
        }
        oss << endl;
        oss << "****LOGICAL_OFFSET**** :" << logical_offset.show() << endl;
        oss << "****    LENGTH         :" << length.show() << endl;
        oss << "**** PHYSICAL_OFFSET **:" << physical_offset.show() << endl;
        return oss.str();
    }

    // format: 
    //   [combo bodysize][begin_timestamp][end_timestamp]
    //            [chunkmap bodysize][chunkmap]
    //            [log off bodysize][log off]
    //            [length body size][length ]
    //            [physi off body size][physi off]
    string PatternCombo::serialize()
    {
        string buf;
        header_t combo_bodysize = 0;
        header_t chunkmap_bodysize = 0;
        header_t log_bodysize = 0;
        header_t len_bodysize = 0;
        header_t phy_bodysize = 0;
        
        //serialize the big ones first
        string logbuf = logical_offset.serialize();
        string lenbuf = length.serialize();
        string phybuf = physical_offset.serialize();
        
        chunkmap_bodysize = sizeof(ChunkMap) * chunkmap.size();
        combo_bodysize = sizeof(begin_timestamp)
                         + sizeof(end_timestamp)
                         + sizeof(header_t) * 4
                         + chunkmap_bodysize
                         + logbuf.size()
                         + lenbuf.size()
                         + phybuf.size();
        log_bodysize = logbuf.size();
        len_bodysize = lenbuf.size();
        phy_bodysize = phybuf.size();
        
        appendToBuffer( buf, &combo_bodysize, sizeof(combo_bodysize));
        appendToBuffer( buf, &begin_timestamp, sizeof(begin_timestamp));
        appendToBuffer( buf, &end_timestamp, sizeof(end_timestamp));
        
        appendToBuffer( buf, &chunkmap_bodysize, sizeof(chunkmap_bodysize));
        if ( chunkmap_bodysize > 0 ) {
            appendToBuffer( buf, &chunkmap[0], chunkmap_bodysize );
        }
        
        appendToBuffer( buf, &log_bodysize, sizeof(log_bodysize));
        appendToBuffer( buf, logbuf.c_str(), logbuf.size() );
        appendToBuffer( buf, &len_bodysize, sizeof(len_bodysize));
        appendToBuffer( buf, lenbuf.c_str(), lenbuf.size() );
        appendToBuffer( buf, &phy_bodysize, sizeof(phy_bodysize));
        appendToBuffer( buf, phybuf.c_str(), phybuf.size());
        //cout << "combo serialized" << endl;
        //cout << "combo_bodysize " << combo_bodysize << endl;
        //cout << "original_chunk_id " << original_chunk_id << endl;
        //cout << "log_bodysize: " << log_bodysize << endl;
        //cout << "len_bodysize: " << len_bodysize << endl;
        //cout << "phy_bodysize: " << phy_bodysize << endl;
        return buf;       
    }

    void PatternCombo::deSerialize( string buf )
    {
        header_t combo_bodysize;
        header_t chunkmap_bodysize;
        header_t log_bodysize = 0;
        header_t len_bodysize = 0;
        header_t phy_bodysize = 0;
        int cur_pos = 0;
        string tmpbuf;
        
        readFromBuf( buf, &combo_bodysize,    cur_pos, sizeof(combo_bodysize));
        readFromBuf( buf, &begin_timestamp,   cur_pos, sizeof(begin_timestamp));
        readFromBuf( buf, &end_timestamp,     cur_pos, sizeof(end_timestamp));
        
        //cout << "COMBO deSerializ..." << endl;
        //cout << "combo_bodysize " << combo_bodysize << endl;
        //cout << "original_chunk_id " << original_chunk_id << endl;
        readFromBuf( buf, &chunkmap_bodysize, cur_pos, sizeof(chunkmap_bodysize));
        
        if ( chunkmap_bodysize > 0 ) {
            chunkmap.resize( chunkmap_bodysize/sizeof(ChunkMap) );
            readFromBuf( buf, &chunkmap[0], cur_pos, chunkmap_bodysize );
        }

        readFromBuf( buf, &log_bodysize,      cur_pos, sizeof(log_bodysize));
        //cout << "log_bodysize: " << log_bodysize << endl;

        if ( log_bodysize > 0 ) {
            tmpbuf.resize( log_bodysize );
            readFromBuf( buf, &tmpbuf[0],     cur_pos, log_bodysize);
            logical_offset.deSerialize( tmpbuf );
        }
        
        readFromBuf( buf, &len_bodysize,      cur_pos, sizeof(len_bodysize));
        //cout << "len_bodysize: " << len_bodysize << endl;
        if ( len_bodysize > 0 ) {
            tmpbuf.resize( len_bodysize );
            readFromBuf( buf, &tmpbuf[0],     cur_pos, len_bodysize );
            length.deSerialize( tmpbuf );
        }
        
        readFromBuf( buf, &phy_bodysize,      cur_pos, sizeof(phy_bodysize));
        //cout << "phy_bodysize: " << phy_bodysize << endl;
        if ( phy_bodysize > 0 ) {
            tmpbuf.resize( phy_bodysize );
            readFromBuf( buf, &tmpbuf[0],     cur_pos, phy_bodysize );
            physical_offset.deSerialize( tmpbuf );
        }
        //cout << "COMBO deSerialized" << endl;
    }

    PatternCombo::~PatternCombo()
    {
        // clean up 
        logical_offset.freeChildren();
        length.freeChildren();
        physical_offset.freeChildren();
    }

    void PatternCombo::append( const PatternCombo &other )
    {
        this->logical_offset.append( &other.logical_offset);
        this->length.append( &other.length );
        this->physical_offset.append( &other.physical_offset );
        begin_timestamp = max( begin_timestamp,
                               other.begin_timestamp );
        end_timestamp = max( end_timestamp,
                             other.end_timestamp );
        // append to this
        chunkmap.insert( chunkmap.end(),
                         other.chunkmap.begin(),
                         other.chunkmap.end() );
    }

    //Get how many offsets are in this combo
    int PatternCombo::getNumOfVal()
    {
        assert(logical_offset.children.size() > 0);
        return logical_offset.children.back()->getNumOfDeltas()+1;
    }

    
    bool PatternCombo::getDetailsByLogicalOff( const off_t logical,
                                 off_t rlength,
                                 off_t rphysical_offset,
                                 pid_t rorigin_id )
    {
        
    }

    bool PatternCombo::expandBadCompression()
    {
        bool expanded = false;
        if ( ! logical_offset.isCompressionRatioGood() ) {
            logical_offset.expandMe();
            expanded = true;
        }
        if ( ! length.isCompressionRatioGood() ) {
            length.expandMe();
            expanded = true;
        }
        if ( ! physical_offset.isCompressionRatioGood() ) {
            physical_offset.expandMe();
            expanded = true;
        }
        return expanded;
    }

    void PatternCombo::saveToFile( const int fd )
    {
        string buf = serialize();
        if ( buf.size() > 0 ) {
            Util::Writen(fd, buf.c_str(), buf.size() );
        }
    }

    void PatternCombo::clear() 
    {
        chunkmap.clear();
        logical_offset.clear();
        length.clear();
        physical_offset.clear();
    }

    ChunkMap PatternCombo::getChunkByPos( const int pos )
    {
        vector<ChunkMap>::const_iterator it;
        int sum = 0;
        for ( it = chunkmap.begin() ;
              it != chunkmap.end() ;
              it++ )
        {
            if ( sum <= pos && pos < sum + it->cnt ) {
                return *it;
            } else {
                sum += it->cnt;
            }
        }
    }

    // if it contains, return true and length and physical_offset
    bool PatternCombo::contains( const off_t logical, 
                                 off_t &ological,
                                 off_t &olength,
                                 off_t &ophysical_offset,
                                 pid_t &id) 
    {
        //mlog(IDX_WARN, "Entering %s", __FUNCTION__ );
        if ( logical_offset.isLeaf() ) {
            //mlog(IDX_WARN, "isLeaf()");
            // this is the messies
            assert(0);
        } else {
            //mlog(IDX_WARN, "not Leaf()");
            assert( logical_offset.children.size() == 2 ); // only handle this at this time
            //mlog(IDX_WARN, "after assertion");
            // for each leaf in the second child, check if the logical is inside
            vector<DeltaNode *>::const_iterator pit;
            vector<off_t>::const_iterator iit;
            int ppos = 0; // how many elements before this child
            for ( iit =  logical_offset.children[0]->elements.begin(),
                  pit =  logical_offset.children[1]->children.begin();
                  iit != logical_offset.children[0]->elements.end();
                  iit++,pit++ ) 
            {
                //mlog(IDX_WARN, "for loop, input logical is:%lld, init is %lld", 
                //        logical, *iit);
                // init: *iit
                // seq: pit->elements
                // cnt: pit->cnt
                off_t offset_init = *iit; // for short

                if ( logical < offset_init ) {
                    //mlog(IDX_WARN, "logical < offset_init. %lld<%lld", logical, offset_init);
                } else if (  pit ==  logical_offset.children[1]->children.end()
                     || (*pit)->elements.size() * (*pit)->cnt <= 1
                     || offset_init == logical
                     ) {
                    //mlog(IDX_WARN, "check only the init");
                    off_t len = length.recoverPos( ppos );
                    if ( isContain( logical, offset_init, len ) ) {
                        ological = offset_init;
                        olength = len;
                        ophysical_offset = physical_offset.recoverPos( ppos );
                        id = getChunkByPos(ppos).new_chunk_id;
                        return true;
                    }
                } else {
                    //ostringstream oss;
                    //oss << "CHECKING init:" << offset_init << endl 
                    //    << (*pit)->show() << endl;
                    //mlog(IDX_WARN, "%s", oss.str().c_str());
              
                    off_t delta_sum = sumVector( (*pit)->elements );
                    off_t roffset = logical - offset_init;
                    off_t col = roffset % delta_sum;
                    off_t row = roffset / delta_sum;
                    int pos = 0;
                    //mlog(IDX_WARN, "col:%lld, row:%lld, delta_sum:%lld", 
                    //        col, row, delta_sum);

                    if ( row >= (*pit)->cnt ) {
                        //mlog(IDX_WARN, "row >= (*pit)->cnt");
                        pos = ppos + (*pit)->elements.size() * (*pit)->cnt - 1;
                        ological = offset_init + 
                                   delta_sum * (*pit)->cnt - (*pit)->elements.back();
                    } else {
                        off_t sum = 0;
                        int col_pos = 0;
                        for ( col_pos = 0;
                              sum <= col;
                              col_pos++ )
                        {
                            sum += (*pit)->elements[col_pos];
                        }
                        sum = sum - (*pit)->elements[col_pos-1];// the last one
                        col_pos--;
                        //mlog(IDX_WARN, "col_pos:%d. sum:%lld", col_pos, sum);
                        ological = offset_init + 
                                   delta_sum * row + sum;
                        pos = ppos + col_pos + row*(*pit)->elements.size();
                    }
                    //mlog(IDX_WARN, "pos: %d, ological: %lld.", 
                    //        pos, ological);
                    olength = length.recoverPos(pos);
                    ophysical_offset = physical_offset.recoverPos(pos);
                    //mlog(IDX_WARN, "olength:%lld, ophysical_offset:%lld.", 
                    //        olength, ophysical_offset);
                    if ( isContain(logical, ological, olength) ) {
                        //mlog(IDX_WARN, "isContain() == true");
                        id = getChunkByPos(ppos).new_chunk_id;
                        return true;
                    }
                }
                ppos += (*pit)->elements.size() * (*pit)->cnt;          
            }
            mlog(IDX_WARN, "cannot find it here");
            return false;
        }
        assert(0);
        
        
        ////////////////////////////////
        int size = logical_offset.children.back()->getNumOfDeltas() + 1; //TODO:WRNING. This is not working for many other case.

        int i;
        int cur_chunk = 0;
        int cur_chunk_inside = 0;
        for ( i = 0 ; i < size ; i++, cur_chunk_inside++ ) {
            assert(cur_chunk < chunkmap.size());
            if ( cur_chunk_inside == chunkmap[cur_chunk].cnt ) {
                cur_chunk++;
                cur_chunk_inside = 0;
            }

            off_t lo = logical_offset.recoverPos(i);
            off_t le = length.recoverPos(i);
            off_t ph = physical_offset.recoverPos(i);

            if ( isContain(logical, lo, le) ) {
                ological = lo;
                olength = le;
                ophysical_offset = ph;
                assert(cur_chunk < chunkmap.size());
                id = chunkmap[cur_chunk].new_chunk_id; 
                return true;
            }
            
        }
        return false;
    }

    ////////////////////////////////////////////////////////////////
    //  PatternUnit
    ////////////////////////////////////////////////////////////////
    string PatternUnit::show() 
    {
        ostringstream oss;

        if ( seq.size() == 0 || cnt == 0 )
            return oss.str();

        oss << ",(";
        vector<off_t>::iterator it;
        for ( it = seq.begin();
                it != seq.end();
                it++ )
        {
            oss << *it ;
            if ( it + 1 != seq.end() ) {
                // not the last one
                oss << ",";
            }
        }
        oss << ")";
        oss << "^" << cnt;
        return oss.str();       
    }


    ////////////////////////////////////////////////////////////////
    //  PatternHeadUnit
    ////////////////////////////////////////////////////////////////
    string PatternHeadUnit::show()
    {
        ostringstream oss;
        oss << init;
        oss << PatternUnit::show();
        return oss.str();       
    }

    PatternHeadUnit::PatternHeadUnit (off_t x)
        :init(x)
    {
    }



    ////////////////////////////////////////////////////////////////
    //  PatternBlock
    ////////////////////////////////////////////////////////////////

    string PatternBlock::show()
    {
        ostringstream oss;

        oss << "[";
        vector<PatternHeadUnit>::iterator it;
        for ( it = block.begin();
                it != block.end();
                it++ )
        {
            oss << it->show();
            if ( it+1 != block.end() ) {
                oss << ";";
            }
        }
        oss << "]";
        return oss.str();
    }



    ////////////////////////////////////////////////////////////////
    //  PatternAnalyzer
    ////////////////////////////////////////////////////////////////

    // input: many PatternUnit (just offset, or PatternHeadUnit)
    // output: a PatternChunk describing the pattern:
    //            with two blocks if pattern found
    //            with only one if not found
    PatternChunk PatternAnalyzer::generatePatterns( PatternBlock &pblock )
    {

    }


}


