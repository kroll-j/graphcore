// Graph Processor core.
// (c) Wikimedia Deutschland, written by Johannes Kroll in 2011, 2012
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

#include <cstring>
#include <cstdarg>
#include <cstdio>
#include <iostream>
#include <vector>
#include <limits>
#include <deque>
#include <queue>
#include <set>
#include <map>
#include <algorithm>
#include <functional>
#include <unordered_map>
#include <exception>
#include <stdexcept>

#include <stdint.h>
#include <sys/time.h>
#include <pthread.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <libintl.h>

#ifdef DEBUG_COMMANDS
#include <malloc.h>
#endif

#include "clibase.h"


// calculate average successors/predecessors per node in the stats command. 
// this takes a little long, so it's disabled.
// #define AVGNEIGHBORS 

// in add-arcs, use mark+remove to remove duplicates from data set instead of erase(). fast.
#define DUPCHECK_MARKRM

// in add-arcs, move duplicates upwards, then erase. slow, don't use.
//#define DUPCHECK_MOVERM

// in remove-arcs, use mark+remove instead of container's erase() method.
#define REMOVEARCS_MARKRM

// in replace-*, use mark+remove instead of container's erase() method.
#define REPLACENEIGHBORS_MARKRM


enum CommandStatus
{
    CORECMDSTATUSCODES
};


#ifndef _
#define _(string) gettext(string)
#endif

#define U32MAX (0xFFFFFFFF)


// time measurement
double getTime()
{
    timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec*0.000001;
}

// check whether we are running interactive or not. interactive mode uses readline.
bool isInteractive()
{
    return isatty(STDOUT_FILENO) && isatty(STDIN_FILENO);
}


struct BasicArc
{
	uint32_t tail, head;
	
	enum { NODE_MAX= 0xFFFFFFFF };

	BasicArc(): tail(0), head(0) {}
	BasicArc(uint32_t _tail, uint32_t _head): tail(_tail), head(_head) {}
	bool operator< (BasicArc a) const
	{
		return (a.tail==tail? a.head<head: a.tail<tail);
	}
	bool operator== (BasicArc a) const
	{
		return (a.tail==tail && a.head==head);
	}
	
	// save this arc to a file in text format
	bool serialize(FILE *f)
	{
		return fprintf(f, "%u, %u\n", tail, head) > 0;
	}
	
	// load arc from file
	bool deserialize(FILE *f)
	{
		unsigned long t, h;
		if(fscanf(f, "%lu, %lu\n", &t, &h)==2)
		{
			tail= t; head= h;
			return true;
		}
		return false;
	}
};

#define DIGRAPH_FILEDUMP_ID	"CatScanDump-01"

template<typename arc=BasicArc> class Digraph
{
	public:
        Digraph(): sortedSize(0)
        {
        }

        ~Digraph()
        {
        }
		
		// simple ascii dump format:
		// <DIGRAPH_FILEDUMP_ID>\n
		// <number of arcs>\n
		// tail, head\n
		// tail, head\n ...
		
		// dump the graph to a file.
		bool serialize(const char *filename, std::string& error)
		{
			FILE *f= fopen(filename, "w");
			if(!f) { error= strerror(errno); return false; }
			if(fprintf(f, "%s\n%u\n", DIGRAPH_FILEDUMP_ID, size())<0)
			{ error= strerror(errno); fclose(f); return false; }
			for(ArcContainerIterator it= arcsByHead.begin(); it!=arcsByHead.end(); it++)
			{
				if(!it->serialize(f))
				{ error= strerror(errno); fclose(f); return false; }
			}
			fclose(f);
			return true;
		}
		
		// load graph from a dump file
		bool deserialize(const char *filename, std::string& error)
		{
			FILE *f= fopen(filename, "r");
			if(!f) { error= strerror(errno); return false; }
			char id[100];
			if(fscanf(f, "%99s\n", id)!=1 || strcmp(id, DIGRAPH_FILEDUMP_ID)!=0)
			{ error= _("could not read matching file format id"); fclose(f); return false; }
			size_t newsize;
			if(fscanf(f, "%zu\n", &newsize)!=1)
			{ error= strerror(errno); fclose(f); return false; }
			clear();
			size_t i;
			arc a;
			for(i= 0; i<newsize; i++)
			{
				if(!a.deserialize(f))
				{ error= strerror(errno); fclose(f); resort(); return false; }
				addArc(a, false);
			}
			resort();
			fclose(f);
			return true;
		}

        // add an arc to the graph
        void addArc(arc a, bool doSort= true)
        {
            ArcContainerIterator lb= lower_bound(arcsByHead.begin(), arcsByHead.begin()+sortedSize, a, compByHead);
            if(lb!=arcsByHead.begin()+sortedSize && *lb==a) return;
            arcsByHead.push_back(a);
            arcsByTail.push_back(a);
            if(doSort) resort(arcsByHead.size()-1);
        }

        // add an arc to the graph
        void addArc(uint32_t tail, uint32_t head, bool doSort= true)
        {
            addArc( arc(tail, head), doSort );
        }

        // clear the graph model
        void clear()
        {
            arcsByTail.clear();
            arcsByHead.clear();
            sortedSize= 0;
        }

        // re-sort arcs starting with given index
        void resort(uint32_t begin= 0)
        {
            threadedSort(begin);
            sortedSize= size();
        }

        // return number of arcs in graph
        uint32_t size() const
        {
            return arcsByTail.size();
        }

        enum NodeRelation
        {
            NEIGHBORS= 0, PREDECESSORS, DESCENDANTS
        };
        // breitensuche / breadth-first-search
        void doBFS(vector<uint32_t> &resultNodes, map<uint32_t,uint32_t> &niveau,
                   uint32_t startNode, uint32_t depth, NodeRelation searchType= NEIGHBORS)
        {
            NeighborIterator it(*this);
            it.startNeighbors(startNode);
            if(it.finished()) return;	// node does not exist
            queue<uint32_t> Q;
            niveau.insert(make_pair(startNode, 0));
            resultNodes.push_back(startNode);
            Q.push(startNode);
            while(Q.size())
            {
                uint32_t next= Q.front();
                uint32_t curNiveau= niveau.find(next)->second;
                if(curNiveau==depth) break;
                Q.pop();
                it.start(next, searchType);
                for(; !it.finished(); ++it)
                {
                    uint32_t neighbor= *it;
                    if(niveau.find(neighbor)==niveau.end())
                    {
                        niveau.insert(make_pair(neighbor, curNiveau+1));
                        resultNodes.push_back(neighbor);
                        Q.push(neighbor);
                    }
                }
            }
        }


        struct BFSnode
        {
            uint32_t niveau;        // bfs node niveau (equals length of path to start node)
            uint32_t pathNext;      // next node upwards in the search tree
            BFSnode():
                niveau(0), pathNext(0) { }
            BFSnode(uint32_t _niveau, uint32_t _pathNext):
                niveau(_niveau), pathNext(_pathNext) { }
        };

        // breadth-first search
        // walk search tree until COMPARE()(node, compArg) returns true
        // returns the node that matched, or 0
        template<typename COMPARE>
            uint32_t doBFS2(uint32_t startNode, uint32_t compArg, uint32_t depth,
                            vector<uint32_t> &resultNodes,
                            map<uint32_t,BFSnode> &nodeInfo,
                            NodeRelation searchType= PREDECESSORS)
        {
            NeighborIterator it(*this);
            it.startNeighbors(startNode);
            if(it.finished()) return 0;	// node does not exist
            if(COMPARE()(*this, startNode, compArg)) return startNode;  // empty path
            queue<uint32_t> Q;
            resultNodes.push_back(startNode);
            nodeInfo[startNode]= BFSnode(0, 0);
            Q.push(startNode);
            while(Q.size())
            {
                uint32_t nextNode= Q.front();
                uint32_t curNiveau= nodeInfo.find(nextNode)->second.niveau;
                if(curNiveau==depth) break;
                Q.pop();
                it.start(nextNode, searchType);
                for(; !it.finished(); ++it)
                {
                    uint32_t neighbor= *it;
                    if(nodeInfo.find(neighbor)==nodeInfo.end()) // if we didn't already visit this node
                    {
                        Q.push(neighbor);
                        // insert this node
                        resultNodes.push_back(neighbor);
                        nodeInfo[neighbor]= BFSnode(curNiveau+1, nextNode);
                        if(COMPARE()(*this, neighbor, compArg)) return neighbor;
                    }
                }
            }
            return 0;
        }

        // does this node have any predecessors?
        bool hasPredecessor(uint32_t node)
        {
            ArcContainerIterator it= findArcByHead(node);
            return (it!=arcsByHead.end() && it->head==node);
        }

        // does this node have any descendants?
        bool hasDescendant(uint32_t node)
        {
            ArcContainerIterator it= findArcByTail(node);
            return (it!=arcsByTail.end() && it->tail==node);
        }

        // comparison operators for search function
        struct findNode
        {
            bool operator() (Digraph &graph, uint32_t node, uint32_t compArg)
            { return node==compArg; }
        };
        struct findRoot
        {
            bool operator() (Digraph &graph, uint32_t node, uint32_t compArg)
            { return !graph.hasPredecessor(node); }
        };
        struct findAll
        {
            bool operator() (Digraph &graph, uint32_t node, uint32_t compArg)
            { return false; }
        };


        // find all roots/leaves in this graph
        void findRoots(vector<uint32_t> &result)
        {
            ArcContainerIterator it= arcsByTail.begin();
            while(it!=arcsByTail.end())
            {
                uint32_t node= it->tail;
                if(!hasPredecessor(node)) result.push_back(node);
                while(it->tail==node && it!=arcsByTail.end()) it++;
            }
        }

        void findLeaves(vector<uint32_t> &result)
        {
            ArcContainerIterator it= arcsByHead.begin();
            while(it!=arcsByHead.end())
            {
                uint32_t node= it->head;
                if(!hasDescendant(node)) result.push_back(node);
                while(it->head==node && it!=arcsByHead.end()) it++;
            }
        }


        // erase an arc from the graph
        bool eraseArc(uint32_t tail, uint32_t head)
        {
            ArcContainerIterator it;
            arc value= arc(tail, head);
            bool found= false;

            it= lower_bound(arcsByHead.begin(), arcsByHead.end(), value, compByHead);
            if( it!=arcsByHead.end() && *it==value )
                arcsByHead.erase(it),
                found= true;

            it= lower_bound(arcsByTail.begin(), arcsByTail.end(), value, compByTail);
            if( it!=arcsByTail.end() && *it==value )
                arcsByTail.erase(it),
                found= true;

            if(found) sortedSize--;
            return found;
        }

        // queue an arc for removal. returns true if the arc was found and successfully queued.
        // call this for all arcs to be removed, 
        // then call removeQueuedArcs() to complete the operation.
        // this method is more efficient than calling eraseArc() repeatedly.
        // because this stores an index referring to the arc internally, 
        // there must not be any write accesses to the graph 
        // between calls to queueArcForRemoval() and removeQueuedArcs().
        bool queueArcForRemoval(uint32_t tail, uint32_t head)
        {
            arc value(tail,head);
            
            // we can't just mark the arcs with a special value here (such as the highest possible 
            // integer for both tail and head) because that would break the container's ordering.
            // so, we first queue the indices of arcs to be removed.
            auto findArc= [&](ArcContainer& arcs, bool(*compFn)(arc,arc)) -> ssize_t
            {
                ArcContainerIterator it= lower_bound(arcs.begin(), arcs.end(), 
                    value, compFn);
                if( it!=arcs.end() && *it==value )
                    return it-arcs.begin();
                return -1;
            };
            
            ssize_t bh= findArc(arcsByHead, compByHead), bt= findArc(arcsByTail, compByTail);
            if(bh<0||bt<0) return false;
            
            arcRemovalQueueBH.push_back(bh);
            arcRemovalQueueBT.push_back(bt);
            
            return true;
        }
        
        int removeQueuedArcs()
        {
            arc markVal(arc::NODE_MAX, arc::NODE_MAX);
            auto mark= [&](ArcContainer& arcs, deque<size_t>& q) -> size_t
            {
                size_t minIdx= arcs.size();
                while(q.size())
                {
                    size_t idx= q.front();
                    if(idx<minIdx) minIdx= idx;
                    arcs[idx]= markVal;
                    q.pop_front();
                }
                return minIdx;
            };
            
            size_t oldSize= size();
            // now mark all arcs queued for removal
            size_t minIdx= mark(arcsByHead, arcRemovalQueueBH);
            minIdx= min(minIdx, mark(arcsByTail, arcRemovalQueueBT));
            // sort, so that marked arcs end up at containers' ends
            // only sort values from the smallest index of all removed arcs
            resort(minIdx);
            // resort also took care of removing all marked arcs because they are duplicates --
            // except the last one
            if(arcsByHead.size() && arcsByHead.back()==markVal) arcsByHead.pop_back();
            if(arcsByTail.size() && arcsByTail.back()==markVal) arcsByTail.pop_back();
            sortedSize= size();
            return oldSize-size();
        }
        
        // replace predecessors (successors=false) or descendants (successors=true) of a node
        bool replaceNeighbors(uint32_t node, vector<uint32_t> newNeighbors, bool successors)
        {
            NeighborIterator it(*this);
            if(successors) it.startDescendants(node);
            else it.startPredecessors(node);
            ArcContainer oldArcs;

            // remove old neighbors of node
            while(!it.checkFinished())
            {
                oldArcs.push_back(it.getArc());
                ++it;
            }
            for(ArcContainerIterator i= oldArcs.begin(); i!=oldArcs.end(); i++)
#ifndef REPLACENEIGHBORS_MARKRM
                eraseArc(i->tail, i->head);
#else
                // todo: keep track of lowest idx for sorting?
                queueArcForRemoval(i->tail, i->head);
            
            resort();
            removeQueuedArcs();
#endif

            // add new neighbors and resort.
            vector<uint32_t>::iterator p;
            int oldSize= arcsByHead.size();
            for(p= newNeighbors.begin(); p!= newNeighbors.end(); p++)
            {
                if(successors) addArc(node, *p, false);
                else addArc(*p, node, false);
            }
            resort(oldSize);
            sortedSize= size();
            return true;
        }

#if 0
        // old broken method, don't use.
        // replace predecessors (successors=false) or descendants (successors=true) of a node
        bool replaceNeighborsX(uint32_t node, vector<uint32_t> newNeighbors, bool successors)
        {
            NeighborIterator it(*this);
            if(successors) it.startDescendants(node);
            else it.startPredecessors(node);
            stable_sort(newNeighbors.begin(), newNeighbors.end());
            vector<uint32_t>::iterator p;

            // replace arcs
            for(p= newNeighbors.begin(); !it.finished() && p!=newNeighbors.end(); ++it, ++p)
            {
//                arc a(it.getArc());
//                (successors? a.head: a.tail)= *p;
//                // only replace the neighbor if it doesn't generate a duplicate
//                if(!arcExists(a.tail, a.head)) *(it.getIterator())= a;
                (successors? it.getArc().head: it.getArc().tail)= *p;
            }
            // if old neighbor list is larger than new one
            if(!it.finished())
            {
                // remove the rest
                for(; !it.checkFinished(); )
                {
                    ArcContainerIterator f=
                        (successors? lower_bound(arcsByHead.begin(), arcsByHead.end(), it.getArc(), compByHead):
                                     lower_bound(arcsByTail.begin(), arcsByTail.end(), it.getArc(), compByTail));
                    fprintf(stderr, "removing arc %u,%u\n", f->tail,f->head);
                    if( f==(successors? arcsByHead.end(): arcsByTail.end()) || !(*f==it.getArc()) )
                    {
                        printf("successors=%s arcsByHead.size()=%d arcsByTail.size()=%d sortedSize=%d\n"
                               "*f=%u,%u it.getArc()=%u,%u\n"
                               ,
                               successors? "true": "false", arcsByHead.size(), arcsByTail.size(), sortedSize,
                               f->tail,f->head, it.getArc().tail,it.getArc().head);
                        abort();    // XXX
                        return false;
                    }
                    if(successors)
                        arcsByTail.erase(it.getIterator()),
                        arcsByHead.erase(f);
                    else
                        arcsByTail.erase(f),
                        arcsByHead.erase(it.getIterator());
                    sortedSize--;
                }
            }
            // if new neighbor list is larger than old one
            else if(p!=newNeighbors.end())
            {
                // add new ones and resort
                int oldSize= arcsByHead.size();
                for(; p!=newNeighbors.end(); p++)
                    if(successors) addArc(node, *p, false);
                    else addArc(*p, node, false);
                resort(oldSize);
            }
            sortedSize= size();
            return true;
        }
#endif

        struct statInfo
        {
            string description;
            size_t value;
            statInfo(const char *desc, size_t val): description(desc), value(val) { }
            statInfo(): description(""), value(0) { }
        };
        // calculate some statistics about this graph (for the stats command)
        void getStats(map<string, statInfo> &result)
        {
            result["ArcCount"]= statInfo(_("number of arcs"), arcsByHead.size());
            result["ArcRamKiB"]= statInfo(_("total RAM consumed by arc data, in KiB"), arcsByHead.size()*sizeof(arc)*2/1024);
            bool invalid= false;
            uint32_t size= arcsByHead.size();
            if(size!=arcsByTail.size())
            {
                invalid= true;
                result["SizeTail"]= statInfo(_("tail array size"), arcsByTail.size());
                result["SizeHead"]= statInfo(_("head array size"), arcsByHead.size());
            }
            uint32_t numDups= 0;
            uint32_t minNodeID= U32MAX, maxNodeID= 0;
#ifdef AVGNEIGHBORS // calculate average successors/predecessors per node. this takes a little long.
            map<uint32_t,uint32_t> totalPredecessors;
            map<uint32_t,uint32_t> totalSuccessors;
#endif
            for(uint32_t i= 0; i<size; i++)
            {
                arc &h= arcsByHead[i];
                if(i<size-1 && h==arcsByHead[i+1])
                    numDups++, invalid= true;
                if(h.tail && h.tail<minNodeID) minNodeID= h.tail;
                if(h.head && h.head<minNodeID) minNodeID= h.head;
                if(h.tail>maxNodeID) maxNodeID= h.tail;
                if(h.head>maxNodeID) maxNodeID= h.head;
#ifdef AVGNEIGHBORS
                totalPredecessors[h.head]++;
                totalSuccessors[h.tail]++;
#endif
            }

#ifdef AVGNEIGHBORS
            size_t s= 0;
            for(map<uint32_t,uint32_t>::iterator it= totalPredecessors.begin(); it!=totalPredecessors.end(); it++)
                s+= it->second;
            if(s) s/= totalPredecessors.size();
            result["AvgPredecessors"]= statInfo(_("average predecessors per node"), s);
            s= 0;
            for(map<uint32_t,uint32_t>::iterator it= totalSuccessors.begin(); it!=totalSuccessors.end(); it++)
                s+= it->second;
            if(s) s/= totalSuccessors.size();
            result["AvgSuccessors"]= statInfo(_("average successors per node"), s);
#endif

            result["MinNodeID"]= statInfo(_("lowest node ID"), minNodeID==U32MAX? 0: minNodeID);
            result["MaxNodeID"]= statInfo(_("greatest node ID"), maxNodeID);
            result["NumDups"]= statInfo(_("number of duplicates found (must be zero)"), numDups);
            result["DataInvalid"]= statInfo(_("nonzero if any obvious errors were found in graph data"), invalid);
        }


        // below is intermediate/internal testing stuff unrelated to the spec.


        // check for duplicates
        void checkDups()
        {
            for(uint32_t i= 0; i<size()-1; i++)
            {
                if(arcsByHead[i].tail==arcsByHead[i+1].tail && arcsByHead[i].head==arcsByHead[i+1].head)
                    printf("dup: %d\n", i);
            }
        }

        // generate a complete graph with nodes 1..nNodes
        void generateCompleteGraph(int nNodes)
        {
            uint32_t oldSize= size();
            for(int i= 0; i<nNodes; i++)
            {
                for(int k= 0; k<nNodes; k++)
                    addArc(i+1, k+1, false);
            }
            resort(oldSize);
        }

        // generate some random arcs
        void generateRandomArcs(int numArcs, int maxNodeID)
        {
            arc a;
            int oldSize= arcsByHead.size();
            printf("generating %'d random arcs...\n", numArcs);
            for(int i= 0; i<numArcs; i++)
            {
                a.tail= i%maxNodeID+1;
                a.head= random()%maxNodeID+1;
                addArc(a, false);
                if(i && numArcs>100 && (i % (numArcs/100) == 0))
                    printf("\r%3u", (uint32_t)((uint64_t)i*100/numArcs)), fflush(stdout);
            }
            printf("\rsorting...\n");
            double d= getTime();
            resort(oldSize);
            printf("done in %fs\n", getTime()-d);
        }

        // list arcs by index sorted by head
        void listArcsByHead(uint32_t start, uint32_t end, FILE *outFile= stdout)
        {
            for(uint32_t i= start; i<end && i<arcsByHead.size(); i++)
                fprintf(outFile, "%d, %d\n", arcsByHead[i].tail, arcsByHead[i].head);
        }

        // list arcs by index sorted by tail
        void listArcsByTail(uint32_t start, uint32_t end, FILE *outFile= stdout)
        {
            for(uint32_t i= start; i<end && i<arcsByTail.size(); i++)
                fprintf(outFile, "%d, %d\n", arcsByTail[i].tail, arcsByTail[i].head);
        }
		
		


    protected:
        typedef deque< arc > ArcContainer;
		typedef typename ArcContainer::iterator ArcContainerIterator;
//        typedef vector< arc > ArcContainer;
        ArcContainer arcsByTail, arcsByHead;
        
        // indices into above containers of arcs queued for removal.
        deque<size_t> arcRemovalQueueBH, arcRemovalQueueBT;

        uint32_t sortedSize;

        // helper class for iterating over all predecessors/successors (or both) of a node
        class NeighborIterator
        {
            private:
                ArcContainerIterator it;
                bool byHead, switchToDescendants;
                Digraph &graph;
                uint32_t startNode;
                bool isFinished;

            public:
                NeighborIterator(Digraph &g): graph(g)
                { }

                bool checkFinished()
                {
                    if( (byHead==true && (it==graph.arcsByHead.end() || it->head!=startNode)) ||
                        (byHead==false && (it==graph.arcsByTail.end() || it->tail!=startNode)) )
                        return true;
                    else
                        return false;
                }

                bool finished()
                {
                    return isFinished;
                }

                void startNeighbors(uint32_t startNode)
                {
                    byHead= true;
                    switchToDescendants= true;
                    it= graph.findArcByHead(startNode);
                    this->startNode= startNode;
                    if( (isFinished= checkFinished()) )
                    {
                        byHead= false;
                        it= graph.findArcByTail(startNode);
                        isFinished= checkFinished();
                    }
                }

                void startPredecessors(uint32_t startNode)
                {
                    byHead= true;
                    switchToDescendants= false;
                    it= graph.findArcByHead(startNode);
                    this->startNode= startNode;
                    isFinished= checkFinished();
                }

                void startDescendants(uint32_t startNode)
                {
                    byHead= false;
                    switchToDescendants= false;
                    it= graph.findArcByTail(startNode);
                    this->startNode= startNode;
                    isFinished= checkFinished();
                }

                void start(uint32_t startNode, NodeRelation type)
                {
                    if(type==PREDECESSORS) startPredecessors(startNode);
                    else if(type==DESCENDANTS) startDescendants(startNode);
                    else startNeighbors(startNode);
                }

                void operator++()
                {
                    if(isFinished) return;
                    if(byHead)
                    {
                        if(++it==graph.arcsByHead.end() || it->head!=startNode)
                        {
                            if(!switchToDescendants)
                            {
                                isFinished= true;
                                return;
                            }
                            if( (it= graph.findArcByTail(startNode))==graph.arcsByTail.end() || it->tail!=startNode )
                                isFinished= true;
                            else
                                byHead= false;
                        }
                    }
                    else
                    {
                        if(++it==graph.arcsByTail.end() || it->tail!=startNode)
                            isFinished= true;
                    }
                }

                uint32_t operator*()
                {
                    if(isFinished) return 0;
                    else return (byHead? it->tail: it->head);
                }

                arc &getArc()
                {
                    if(isFinished) return graph.arcsByHead[0];
                    return *it;
                }

                ArcContainerIterator getIterator()
                {
                    return it;
                }
        };
        friend class NeighborIterator;

        // helper function for sorting arcs by tail
        static bool compByTail(arc a, arc b)
        {
            return (a.tail==b.tail? a.head<b.head: a.tail<b.tail);
        }

        // helper function for sorting arcs by head
        static bool compByHead(arc a, arc b)
        {
            return (a.head==b.head? a.tail<b.tail: a.head<b.head);
        }

        // find the position of first arc with given head (lower bound)
        ArcContainerIterator findArcByHead(uint32_t head)
        {
            arc value(0, head);
            return lower_bound(arcsByHead.begin(), arcsByHead.end(), value, compByHead);
        }

        // find the position of first arc with given tail (lower bound)
        ArcContainerIterator findArcByTail(uint32_t tail)
        {
            arc value(tail, 0);
            return lower_bound(arcsByTail.begin(), arcsByTail.end(), value, compByTail);
        }

        struct sorterThreadArg
        {
            ArcContainer& arcs;
            size_t begin, mergeBegin, end;
            bool (*compFunc)(arc a, arc b);
        };
        // thread function for sorting
        static void *sorterThread(void *a)
        {
            sorterThreadArg *arg= (sorterThreadArg*)a;
            doMerge(arg->arcs, arg->begin, arg->mergeBegin, arg->end, arg->compFunc);
            return 0;
        }
        // re-sort arcs in 2 threads
        void threadedSort(int mergeBegin)
        {
            pthread_t threadID;
            sorterThreadArg arg= { arcsByHead, 0, mergeBegin, arcsByHead.size(), compByHead };
            pthread_create(&threadID, 0, sorterThread, &arg);
            doMerge(arcsByTail, 0, mergeBegin, arcsByTail.size(), compByTail);
            pthread_join(threadID, 0);
        }
        
        static void moveToEnd(ArcContainer& arcs, int a, int end)
        {
            for(int i= a; i+1<end; i++)
                std::swap(arcs.begin()[i], arcs.begin()[i+1]); 
        }

        // helper function: merge & resort
        // also removes duplicates in the given range
        static void doMerge(ArcContainer &arcs, int begin, int mergeBegin, int end,
                            bool (*compFunc)(arc a, arc b))
        {
            stable_sort(arcs.begin()+mergeBegin, arcs.begin()+end, compFunc);

            unsigned numDups= 0;
            for(int i= mergeBegin; i<end-1; i++)
                if( arcs[i] == arcs[i+1] )
                {
#if defined(DUPCHECK_MOVERM)
                    moveToEnd(arcs, i, end);
                    i--;
                    end--;
#elif defined(DUPCHECK_MARKRM)
                    // erase()ing from the middle is slow 
                    // just mark the duplicate to be removed below
                    arcs[i].tail= arcs[i].head= 0xFFFFFFFF;
#else
                    arcs.erase(arcs.begin()+i);
                    i--;
                    end--;
#endif
                    numDups++;
                }
            if(numDups)
            {
#if defined(DUPCHECK_MOVERM)
                // erase the duplicate arcs which were moved to the end of the container
                arcs.erase(arcs.end()-numDups, arcs.end());
#elif defined(DUPCHECK_MARKRM)
                // sort arcs so that marked duplicates end up at the end of the container
                stable_sort(arcs.begin()+mergeBegin, arcs.begin()+end, compFunc);
                // erasing at the end is fast
                arcs.erase(arcs.end()-numDups, arcs.end());
                end-= numDups;
#endif
//                fprintf(stderr, "%u dups in merge set, begin=%d mergeBegin=%d end=%d size()=%zu\n", 
//                    numDups, begin, mergeBegin, end, arcs.size());
            }

            inplace_merge(arcs.begin()+begin, arcs.begin()+mergeBegin, arcs.begin()+end, compFunc);
        }
        
        friend class ccRMStuff; // can read arc data directly for debugging.
};
typedef Digraph<BasicArc> BDigraph;



class CoreCliCommand: public CliCommand
{
    public:
        string getName() { return name; }
        void setName(string n) { name= n; }
        void syntaxError()
        {
            CliCommand::syntaxError();
            if(getReturnType()==RT_OTHER) cout << lastStatusMessage;
        }

    private:
        string name;
};



// base classes for cli commands. derive commands from these.
// YourCliCommand::execute() must return the appropriate CommandStatus error code.

// cli commands which do not return a data set.
class CliCommand_RTVoid: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_NONE; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)= 0;
};

// cli commands which return a node list data set.
class CliCommand_RTNodeList: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_NODE_LIST; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                             vector<uint32_t> &result)= 0;
};

// cli commands which return an arc list data set.
class CliCommand_RTArcList: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_ARC_LIST; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                                      vector<BasicArc> &result)= 0;
};

// cli commands which return some other data set. execute() must print the result to outFile.
class CliCommand_RTOther: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_OTHER; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)= 0;
};



class CoreCli: public Cli
{
    public:
        typedef unordered_map<string, string> MetaMap;
        MetaMap meta;
    
        CoreCli(BDigraph *g);

        // read and execute commands from stdin until eof or quit command
        void run()
        {
            char *command= 0;
            FILE *inRedir= 0, *outRedir= 0;
            bool commandHasDataSet;
            while(!doQuit)
            {
                fflush(stdout);
                if(inRedir) fclose(inRedir), inRedir= 0;
                if(outRedir) fclose(outRedir), outRedir= 0;
                if( (command= getLine())==0 ) return;
                char *completeCommand= strdup(command);
                if(!completeCommand) { printf(ERROR_STR " out of memory.\n"); return; }
                char *d= strchr(command, '>');
                if(d)
                {
                    *d++= 0;
                    if(!(outRedir= fopen(getRedirFilename(d), "w")))
                    {
                        printf( "%s %s", FAIL_STR, _("couldn't open output file\n") );
                        continue;
                    }
                }
                d= strchr(command, '<');
                if(d)
                {
                    *d++= 0;
                    if(!(inRedir= fopen(getRedirFilename(d), "r")))
                    {
                        printf( "%s %s", FAIL_STR, _("couldn't open input file\n") );
                        continue;
                    }
                }
                int cpos= 0;
                if( (commandHasDataSet= lineIndicatesDataset(command, &cpos)) )
                    command[cpos]= 0;

                if(strlen(command))
                    execute(command, commandHasDataSet, inRedir, outRedir),
                    add_history(completeCommand);
                free(command);
                free(completeCommand);
            }
        }

        void quit() { doQuit= true; }



    protected:
        BDigraph *myGraph;

        bool doQuit;

        // read a line from stdin using readline if appropriate
        // return 0 on error
        char *getLine()
        {
            if(isInteractive()) return filterNewlines(readline("> "));
            return Cli::getLine();
        }

        // execute a command
        // inRedir/outRedir are non-null if input/output should be redirected.
        CommandStatus execute(char *command, bool hasDataSet, FILE *inRedir, FILE *outRedir)
        {
            try
            {
                vector<string> words= splitString(command);
                if(words.size()<1) return CMD_FAILURE;

                FILE *outFile= (outRedir? outRedir: stdout);
                FILE *inFile= (inRedir? inRedir: stdin);
                CliCommand *cmd= findCommand(words[0]);

                string opstring;
                vector<string> words2;
                splitByOperator(words, opstring, words2);

                if(cmd)
                {
                    if(!opstring.empty() && cmd->getReturnType()!=CliCommand::RT_NODE_LIST)
                    {
                        cout << FAIL_STR << " " << _("operators not available for this return type.") << endl;
                        return CMD_FAILURE;
                    }
                    CommandStatus status= CMD_SUCCESS;
                    switch(cmd->getReturnType())
                    {
                        case CliCommand::RT_NODE_LIST:
                        {
                            vector<uint32_t> result;
                            status= ((CliCommand_RTNodeList*)cmd)->execute(words, this, myGraph, hasDataSet, inFile, outFile, result);
                            if(!opstring.empty())
                            {
                                // operator handling
                                if(status==CMD_SUCCESS||status==CMD_NONE)
                                {
                                    CliCommand *cmd2= findCommand(words2[0]);
                                    if(!cmd2)
                                    { printf("%s %s '%s'.\n", FAIL_STR, _("no such command"), words2[0].c_str()); break; }
                                    if(cmd2->getReturnType()!=cmd->getReturnType())
                                    { printf("%s %s.\n", FAIL_STR, _("return type mismatch")); break; }
                                    vector<uint32_t> result2;
                                    CommandStatus status2=
                                        ((CliCommand_RTNodeList*)cmd2)->execute(words2, this, myGraph, hasDataSet, inFile, outFile, result2);
                                    if(status2==CMD_SUCCESS||status2==CMD_NONE)
                                    {
                                        stable_sort(result.begin(), result.end());
                                        stable_sort(result2.begin(), result2.end());
                                        vector<uint32_t> mergeResult;
                                        vector<uint32_t>::iterator end;
                                        if(opstring=="&&")
                                        {
                                            mergeResult.resize(min(result.size(), result2.size()));
                                            end= set_intersection(result.begin(), result.end(),
                                                                  result2.begin(), result2.end(),
                                                                  mergeResult.begin());
                                        }
                                        else if(opstring=="&&!")
                                        {
                                            mergeResult.resize(result.size());
                                            end= set_difference(result.begin(), result.end(),
                                                                result2.begin(), result2.end(),
                                                                mergeResult.begin());
                                        }
    // && should return NONE *only* if one of the operands return NONE.
    // &&! should return NONE if the left operand is NONE. NONE on the right side is treated like an empty set.
                                        if( (opstring=="&&" && (status==CMD_NONE||status2==CMD_NONE)) ||
                                            (opstring=="&&!" && (status==CMD_NONE)) )
                                            cout << "NONE." << endl;
                                        else
                                        {
                                            cout << SUCCESS_STR <<
                                                " L: " << result.size() << " R: " << result2.size() << " -> " << end-mergeResult.begin() <<
                                                (outRedir? "": ":") << endl;
                                            for(vector<uint32_t>::iterator it= mergeResult.begin(); it!=end; it++)
                                                fprintf(outFile, "%u\n", *it);
                                            fprintf(outFile, "\n");
                                        }
                                    }
                                    else { cout << cmd2->getStatusMessage(); break; }
                                }
                                else { cout << cmd->getStatusMessage(); break; }
                            }
                            else
                            {
                                cout << cmd->getStatusMessage();
                                if(status==CMD_SUCCESS)
                                {
                                    for(size_t i= 0; i<result.size(); i++)
                                        fprintf(outFile, "%u\n", result[i]);
                                    fprintf(outFile, "\n");
                                }
                            }
                            break;
                        }
                        case CliCommand::RT_ARC_LIST:
                        {
                            vector<BasicArc> result;
                            status= ((CliCommand_RTArcList*)cmd)->execute(words, this, myGraph, hasDataSet, inFile, outFile, result);
                            cout << cmd->getStatusMessage();
                            if(status==CMD_SUCCESS)
                            {
                                for(size_t i= 0; i<result.size(); i++)
                                    fprintf(outFile, "%u,%u\n", result[i].tail, result[i].head);
                                fprintf(outFile, "\n");
                            }
                            break;
                        }
                        case CliCommand::RT_OTHER:
                            status= ((CliCommand_RTOther*)cmd)->execute(words, this, myGraph, hasDataSet, inFile, outFile);
                            break;
                        case CliCommand::RT_NONE:
                            if(outFile!=stdout)
                                printf("%s %s", FAIL_STR, _("output redirection not possible for this command.\n")),
                                status= CMD_FAILURE;
                            else
                                status= ((CliCommand_RTVoid*)cmd)->execute(words, this, myGraph, hasDataSet, inFile),
                                cout << cmd->getStatusMessage();
                            break;
                    }
                    return status;
                }
                if(hasDataSet)
                {   // command not found, slurp data set anyway as per spec.
                    vector< vector<uint32_t> > dummyvec;
                    commands[0]->readNodeset(inFile, dummyvec, 0);
                }
                printf("%s %s\n", FAIL_STR, _("no such command."));
                return CMD_FAILURE;
            }
            catch(exception& e) // exceptions *should* be caught further down, but if anything gets here, catch it
            {
                printf("%s %s '%s'\n", ERROR_STR, _("caught exception:"), e.what());
                return CMD_ERROR;
            }
        }
};



///////////////////////////////////////////////////////////////////////////////////////////
// ccListNeighbors
// template class for list-* commands
template<BDigraph::NodeRelation searchType, bool recursive>
    class ccListNeighbors: public CliCommand_RTNodeList
{
    public:
        string getSynopsis()        { return getName() + _(" NODE") + (recursive? _(" DEPTH"): ""); }
        string getHelpText()
        {
            if(recursive) switch(searchType)
            {
                case BDigraph::NEIGHBORS: return _("list NODE and its neighbors recursively up to DEPTH.");
                case BDigraph::PREDECESSORS: return _("list NODE and its predecessors recursively up to DEPTH.");
                case BDigraph::DESCENDANTS: return _("list NODE and its successors recursively up to DEPTH.");
            }
            else switch(searchType)
            {
                case BDigraph::NEIGHBORS: return _("list direct neighbors of NODE.");
                case BDigraph::PREDECESSORS: return _("list direct predecessors of NODE.");
                case BDigraph::DESCENDANTS: return _("list direct successors of NODE.");
            }
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                     vector<uint32_t> &result)
        {
            if( (words.size()!=(recursive? 3: 2)) || hasDataSet ||
                !Cli::isValidNodeID(words[1]) || (recursive && !Cli::isValidUint(words[2])) )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            double d= getTime();
            #if 1
            map<uint32_t,BDigraph::BFSnode> nodeInfo;
            graph->doBFS2<BDigraph::findAll> (Cli::parseUint(words[1]), 0, (recursive? Cli::parseUint(words[2]): 1),
                                             result, nodeInfo, searchType);
            #else
            map<uint32_t,uint32_t> nodeNiveau;
            graph->doBFS(result, nodeNiveau, Cli::parseUint(words[1]),
                         (recursive? Cli::parseUint(words[2]): 1),
                         searchType);
            #endif
            if(!recursive && result.size()) result.erase(result.begin());
            if(recursive && !result.size())
            {
                cliNone(_("Node not found.\n"));
                return CMD_NONE;
            }
            else
            {
                cliSuccess(_("%zu nodes, %fs%s\n"), result.size(), getTime()-d, outFile==stdout? ":": "");
                return CMD_SUCCESS;
            }
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccListNeighborless
// list roots / leaves
template<bool leaves>
    class ccListNeighborless: public CliCommand_RTNodeList
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()
        {
            if(leaves)
                return _("list leaf nodes (nodes without successors).");
            else
                return _("list root nodes (nodes without predecessors).");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                     vector<uint32_t> &result)
        {
            if( words.size()!=1 || hasDataSet )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            if(leaves) graph->findLeaves(result);
            else graph->findRoots(result);
            cliSuccess("%zu %s%s\n", result.size(), (leaves? _("leaf nodes"): _("root nodes")),
                       outFile==stdout? ":": "");
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccHelp
// help command
class ccHelp: public CliCommand_RTOther
{
    private:
        CoreCli *cli;

    public:
        ccHelp(CoreCli *_cli): cli(_cli)
        { }

        string getSynopsis()        { return getName() + _(" [COMMAND] / ") + getName() + _(" operators"); }
        string getHelpText()        { return getName() + _(": list commands") + "\n# " +
                                             getName() + _(" COMMAND: get help on COMMAND") + "\n# " +
                                             getName() + _(" operators: print help on operators"); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if(words.size()>2 || hasDataSet)
            {
                syntaxError();
                return CMD_FAILURE;
            }
            if(words.size()==2)
            {
                if(words[1]=="operators")
                {
                    cliSuccess("%s\n", outFile==stdout? ":": "");
                    cout << lastStatusMessage << _("\
# Operators can be used to combine the output of two commands into one\n\
# data-set. They are used with infox syntax:\n\
# \n\
# <COMMAND> <OPERATOR> <COMMAND>\n\
# \n\
# This way, a composite command is formed. Note that if either operand\n\
# fails, the composite command also fails.\n\
# \n\
# The following operators are currently specified:\n\
# \n\
# intersection (&&):\n\
# The intersection operator takes two operands, both of wich must\n\
# return a set of nodes. The result of the composite command is a set of\n\
# nodes that contains only the nodes that are in both, the result of the\n\
# left operand, and the result of the right. If and only if either\n\
# operand returns NONE, the result is NONE. \n\
# \n\
# subtraction (&&!):\n\
# The subtraction operator takes two operands, both of\n\
# which must return a set of nodes. The result of the composite command is\n\
# a set of nodes that contains only the nodes that are in the result of\n\
# the left operand but not in the result of the right operand. If and only\n\
# if the left operand returns NONE, the result is NONE. If the right\n\
# operand returns NONE, the result is the result of the left operand.\n\n");
                    return CMD_SUCCESS;
                }
                CliCommand *cmd= cli->findCommand(words[1]);
                if(!cmd)
                {
                    cliFailure(_("%s: no such command."), words[1].c_str());
                    cout << lastStatusMessage << endl;
                    return CMD_FAILURE;
                }
                cliSuccess("%s\n", outFile==stdout? ":": "");
                cout << lastStatusMessage << "# " << cmd->getSynopsis() << endl << "# " << cmd->getHelpText() << endl;
            }
            else
            {
                cliSuccess(_("available core commands%s\n"), outFile==stdout? ":": "");
                cout << lastStatusMessage;
                vector<CliCommand*> &commands= cli->getCommands();
                for(unsigned i= 0; i<commands.size(); i++)
                    cout << "# " << commands[i]->getSynopsis() << endl;
            }
            cout << endl;
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccStats
// stats command
class ccStats: public CliCommand_RTOther
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()
        {
            string s= string(_("print some statistics about the graph in the form of a name,value data set.\n")) +
                      "# " + _("names and their meanings:");
            BDigraph graph;
            map<string, BDigraph::statInfo> info;
            graph.getStats(info);
            for(map<string, BDigraph::statInfo>::iterator i= info.begin(); i!=info.end(); i++)
                s+= "\n# " + i->first + "\t" + i->second.description;
            return s;
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if(words.size()!=1 || hasDataSet)
            {
                syntaxError();
                return CMD_FAILURE;
            }
            map<string, BDigraph::statInfo> info;
            graph->getStats(info);
            cliSuccess("%s\n", outFile==stdout? ":": "");
            cout << lastStatusMessage;
            for(map<string, BDigraph::statInfo>::iterator i= info.begin(); i!=info.end(); i++)
                fprintf(outFile, "%s,%zu\n", i->first.c_str(), i->second.value);
            fprintf(outFile, "\n");
            fflush(outFile);
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccAddArcs
// add-arcs command
class ccAddArcs: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " {:|<}"; }
        string getHelpText()        { return _("read a data set of arcs and add them to the graph. empty line terminates the set."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || !(hasDataSet||(inFile!=stdin)))
            {
                syntaxError();
                return CMD_FAILURE;
            }

/*
            uint32_t oldSize= graph->size();
            vector< vector<uint32_t> > dataset;
            if(!readNodeset(inFile, dataset, 2))
                return CMD_FAILURE;

            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                graph->addArc((*i)[0], (*i)[1], false);
            graph->resort(oldSize);
*/

            uint32_t oldSize= graph->size();
            vector<uint32_t> record;
            bool ok= true;
            cliSuccess("\n");
/*
            for(unsigned lineno= 1; ; lineno++)
            {
                record.clear();
                if( !Cli::readNodeIDRecord(inFile, record) )
                {
                    if(ok) cliError(_("error reading data set. strerror(): '%s' (line %u)\n"), strerror(errno), lineno);
                    ok= false;
                }
                else if(record.size()==0)
                {
                    if(!ok) return CMD_ERROR;
                    graph->resort(oldSize);
                    cliSuccess("\n");
                    return CMD_SUCCESS;
                }
                else if(record.size()!=2)
                {
                    if(ok) cliError(_("error reading data set: record size %d, should be 2. (line %u)\n"), record.size(), lineno);
                    ok= false;
                }
                else
                {
                    if(record[0]==0 || record[1]==0) { cliError(_("invalid node ID in line %d\n"), lineno); ok= false; }
                    if(ok) graph->addArc(record[0], record[1], false);
                }
            }
*/
            for(unsigned lineno= 1; ; lineno++)
            {
                record.clear();
                try
                {
                    Cli::readNodeIDRecord(inFile, record); 
                    if(record.size()==0)
                    {
                        if(!ok) return CMD_ERROR;
                        graph->resort(oldSize);
                        cliSuccess("\n");
                        return CMD_SUCCESS;
                    }
                    else if(record.size()!=2)
                    {
                        if(ok) cliError(_("error reading data set: record size %d, should be 2. (line %u)\n"), record.size(), lineno);
                        ok= false;
                    }
                    else
                    {
                        if(record[0]==0 || record[1]==0) { cliError(_("invalid node ID in line %d\n"), lineno); ok= false; }
                        if(ok) graph->addArc(record[0], record[1], false);
                    }
                }
                catch(exception& e)
                {
                    if(ok) cliError(_("error reading data set: '%s' in line %u\n"), e.what(), lineno);
                    ok= false;
                }
            }
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccRemoveArcs
// remove-arcs command
class ccRemoveArcs: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " {:|<}"; }
        string getHelpText()        { return _("read a data set of arcs and remove them from the graph. empty line terminates the set."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=1 || !(hasDataSet||(inFile!=stdin)) )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            vector< vector<uint32_t> > dataset;
            if(!readNodeset(inFile, dataset, 2))
                return CMD_FAILURE;

#ifdef REMOVEARCS_MARKRM
            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                graph->queueArcForRemoval((*i)[0], (*i)[1]);
            graph->removeQueuedArcs();
#else
            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                graph->eraseArc((*i)[0], (*i)[1]);
#endif

            cliSuccess("\n");
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccReplaceNeighbors
// replace-predecessors/replace-successors commands
template<BDigraph::NodeRelation searchType>
    class ccReplaceNeighbors: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + _(" NODE") + " {:|<}"; }
        string getHelpText()
        {
            switch(searchType)
            {
                case BDigraph::PREDECESSORS: return _("read data set of nodes and replace predecessors of NODE with given set.");
                case BDigraph::DESCENDANTS: return _("read data set of nodes and replace successors of NODE with given set.");
                default:    ;
            }
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=2 || !(hasDataSet||(inFile!=stdin)) ||
                !Cli::isValidNodeID(words[1]) )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            vector<uint32_t> newNeighbors;
            vector< vector<uint32_t> > dataset;

            if(!readNodeset(inFile, dataset, 1))
                return CMD_FAILURE;
            newNeighbors.reserve(dataset.size());
            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                newNeighbors.push_back((*i)[0]);

            if(graph->replaceNeighbors(Cli::parseUint(words[1]), newNeighbors, 
                                        searchType==BDigraph::DESCENDANTS))
            {
                cliSuccess("\n");
                return CMD_SUCCESS;
            }
            else
            {
                cliError(_("internal error: BDigraph::replaceNeighbors() failed.\n"));
                return CMD_ERROR;
            }
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccClear
// clear command.
class ccClear: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return _("clear the graph model."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=1 || hasDataSet || (inFile!=stdin) )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            graph->clear();
            cliSuccess("\n");
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccShutdown
// shutdown command.
class ccShutdown: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return _("shutdown the graph processor."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || hasDataSet || (inFile!=stdin))
            {
                syntaxError();
                return CMD_FAILURE;
            }
            cliSuccess(_("shutting down pid %d.\n"), (int)getpid());
            cli->quit();
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccFindPath
// find shortest path between nodes (findRoot==false) or path to nearest root node (findRoot==true)
template<bool findRoot> class ccFindPath: public CliCommand_RTArcList
{
    public:
        string getSynopsis()        { return getName() + (findRoot? " X": " X Y"); }
        string getHelpText()
        {
            if(findRoot)
                return _("find the path from X to nearest root node. return data set of arcs representing the path.");
            else
                return _("find the shortest path from node X to node Y. return data set of arcs representing the path.");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile, vector<BasicArc> &result)
        {
            if( hasDataSet ||
                (findRoot? (words.size()!=2):
                           (words.size()!=3 || !Cli::isValidNodeID(words[2]))) ||
                (!Cli::isValidNodeID(words[1])) )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            vector<uint32_t> nodes;
            map<uint32_t,BDigraph::BFSnode> nodeInfo;
            uint32_t node;

            if(findRoot)
                node= graph->doBFS2<BDigraph::findRoot> (Cli::parseUint(words[1]), 0, U32MAX,
                                                        nodes, nodeInfo);
            else
                node= graph->doBFS2<BDigraph::findNode> (Cli::parseUint(words[2]), Cli::parseUint(words[1]), U32MAX,
                                                        nodes, nodeInfo);
            if(node)
            {
                uint32_t next;
                result.resize(nodeInfo[node].niveau);
                vector<BasicArc>::iterator it= result.begin();
                while((next= nodeInfo[node].pathNext))
                {
                    *it++= BasicArc(node, next);
                    node= next;
                }
                cliSuccess(_("%zu nodes visited, path length %zu%s\n"), nodes.size(), result.size(),
                           outFile==stdout? ":": "");
                return CMD_SUCCESS;
            }
            cliNone("\n");
            return CMD_NONE;
        }
};


#ifdef DEBUG_COMMANDS

///////////////////////////////////////////////////////////////////////////////////////////
// ccListArcs
// list arcs by tail/head (debugging)
template<bool byHead> class ccListArcs: public CliCommand_RTOther
{
    public:
        string getSynopsis()        { return getName() + " INDEX [N]"; }
        string getHelpText()
        {
            return _("debugging: list N arcs starting from INDEX, ") + string(byHead? "sorted by head": "sorted by tail");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if( hasDataSet ||
                (words.size()==3 && !Cli::isValidUint(words[2])) ||
                (words.size()<2 || words.size()>3) ||
                !Cli::isValidUint(words[1]) )
            {
                syntaxError();
                return CMD_FAILURE;
            }


            cliSuccess("%s\n", outFile==stdout? ":": "");
            cout << lastStatusMessage;

            uint32_t start= Cli::parseUint(words[1]),
                     end= (words.size()==3? start+Cli::parseUint(words[2]): graph->size()-start);
            if(byHead) graph->listArcsByHead(start, end, outFile);
            else graph->listArcsByTail(start, end, outFile);

            fprintf(outFile, "\n");
            fflush(outFile);

            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccAddStuff
// (debugging)
class ccAddStuff: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " NUM [MOD=RAND_MAX]"; }
        string getHelpText()
        {
            return _("debugging: add NUM random arcs with tail,head in range 1..MOD directly to the graph");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, 
                                bool hasDataSet, FILE *inFile)
        {
            if( hasDataSet ||
                (words.size()==3 && !Cli::isValidUint(words[2])) ||
                (words.size()<2 || words.size()>3) ||
                !Cli::isValidUint(words[1]) )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            uint32_t num= Cli::parseUint(words[1]);
            uint32_t mod= (words.size()>2? Cli::parseUint(words[2]): RAND_MAX);

            double tStart= getTime();
            uint32_t oldSize= graph->size();
            for(unsigned i= 0; i<num; i++)
            {
                graph->addArc(rand()%mod+1, rand()%mod+1, false);
            }
            double tEndAdd= getTime();
            graph->resort(oldSize);
            double tEnd= getTime();
            
            cliSuccess("added in %.2f sec, merged in %.2f sec\n", tEndAdd-tStart, tEnd-tEndAdd);

            return CMD_SUCCESS;
        }
};

///////////////////////////////////////////////////////////////////////////////////////////
// ccRMStuff
// (debugging)
class ccRMStuff: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " NUM"; }
        string getHelpText()
        {
            return _("debugging: remove NUM random arcs directly from the graph");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, 
                                bool hasDataSet, FILE *inFile)
        {
            if( hasDataSet ||
                words.size()!=2 ||
                !Cli::isValidUint(words[1]) )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            uint32_t num= Cli::parseUint(words[1]);
            
            deque<BasicArc> rmQueue;

            double tStart= getTime();
            uint32_t oldSize= graph->size();
            if(oldSize) 
            {
                // queue them first so that valid comparison between removal methods is possible
                for(unsigned i= 0; i<num; i++)
                {
                    size_t r= rand()%oldSize;
                    rmQueue.push_back(graph->arcsByHead[r]);
                }
                while(rmQueue.size())
                {
                    BasicArc& a= rmQueue.front();
                    rmQueue.pop_front();
#ifdef REMOVEARCS_MARKRM
                    graph->queueArcForRemoval(a.tail, a.head);
                }
                graph->removeQueuedArcs();
#else
                    graph->eraseArc(a.tail, a.head);
                }
#endif
            }
            double tEnd= getTime();
            
            cliSuccess("removed %d arcs in %.2f sec\n", oldSize-graph->size(), tEnd-tStart);
            return CMD_SUCCESS;
        }
};

///////////////////////////////////////////////////////////////////////////////////////////
// ccMallocStats
// (debugging)
class ccMallocStats: public CliCommand_RTOther
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()
        {
            return _("debugging");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if( hasDataSet || words.size()!=1 )
            {
                syntaxError();
                return CMD_FAILURE;
            }

//            printf("Mmapped: %dM used: %dM (%d%%)\n",
//                   gMmappedBytes/(1024*1024), gUsedBytes/(1024*1024), gUsedBytes/(gMmappedBytes/100));

#ifdef __linux__
            malloc_stats();
#else
            printf("malloc_stats() not implemented.\n");
#endif

            return CMD_SUCCESS;
        }
};

#endif // DEBUG_COMMANDS


///////////////////////////////////////////////////////////////////////////////////////////
// ccProtocolVersion
// print PROTOCOL_VERSION. for internal use only.
class ccProtocolVersion: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()
        {
            return _("print PROTOCOL_VERSION. for internal use only.");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( hasDataSet ||
                words.size()!=1 )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            cliSuccess(stringify(PROTOCOL_VERSION) "\n");

            return CMD_SUCCESS;
        }
};




///////////////////////////////////////////////////////////////////////////////////////////
// ccSetMeta
// add or set a free-form variable
class ccSetMeta: public CliCommand_RTVoid
{
    // check for valid variable name. these are the same constraints as for core instance names.
    // [a-zA-Z_-][a-zA-Z0-9_-]*
    bool isValidVariableName(const string& name)
    {
        int sz= name.size();
        if(!sz) return false;
        char c= name[0];
        if( !isupper(c) && !islower(c) && c!='-' && c!='_' ) return false;
            for(size_t i= 0; i<name.size(); i++)
        {
            c= name[i];
            if( !isupper(c) && !islower(c) && !isdigit(c) && c!='-' && c!='_' )
                return false;
        }
        return true;
    }

    public:
        string getSynopsis()        { return getName() + " NAME VALUE"; }
        string getHelpText()        { return _("add or set an arbitrary text variable.\n"
                "# variable names may contain alphabetic characters (a-z A-Z), digits (0-9), hyphens (-) and underscores (_),\n"
                "# and must start with an alphabetic character, a hyphen or an underscore."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=3 || hasDataSet || (inFile!=stdin) )
            {
                syntaxError();
                fflush(stdout);
                return CMD_FAILURE;
            }

            if(!isValidVariableName(words[1]))
            {
                cliFailure(_("invalid variable name (see help)\n"));
                return CMD_FAILURE;
            }
            
            cli->meta[words[1]]= words[2];
            
            cliSuccess("\n");
            
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccGetMeta
// read a free-form variable
class ccGetMeta: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " NAME"; }
        string getHelpText()        { return _("read a named text variable."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=2 || hasDataSet || inFile!=stdin )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            CoreCli::MetaMap::iterator it= cli->meta.find(words[1]);
            if(it==cli->meta.end())
            {
                cliFailure(_("no such variable '%s'.\n"), words[1].c_str());
                return CMD_FAILURE;
            }
            
            cliValue("%s\n", it->second.c_str());
            
            return CMD_VALUE;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccRemoveMeta
// remove a free-form variable
class ccRemoveMeta: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " NAME"; }
        string getHelpText()        { return _("remove the named variable."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=2 || hasDataSet || inFile!=stdin )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            if(cli->meta.find(words[1])==cli->meta.end())
            {
                cliFailure(_("no such variable %s.\n"), words[1].c_str());
                return CMD_FAILURE;
            }
            
            cli->meta.erase(words[1]);
            cliSuccess("\n");
            
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccListMeta
// list all variables
class ccListMeta: public CliCommand_RTOther
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return _("list all variables in this graph."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if( words.size()!=1 || hasDataSet || inFile!=stdin )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            
            cliSuccess(_("%d meta variables:"), (int)cli->meta.size());
            cout << lastStatusMessage << endl;
            
            vector< pair<string,string> > sortedVars;
            sortedVars.reserve(cli->meta.size());
            
            for(auto it= cli->meta.begin(); it!=cli->meta.end(); it++)
                sortedVars.push_back(*it);
            
            std::sort(sortedVars.begin(), sortedVars.end());
            
            for(auto it= sortedVars.begin(); it!=sortedVars.end(); it++)
                cout << it->first << "," << it->second << endl;
            
            cout << endl;
            
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccDumpGraph
// save the graph to a file.
class ccDumpGraph: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " FILENAME"; }
        string getHelpText()        { return _("save the graph to a file."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=2 || hasDataSet || inFile!=stdin )
            {
                syntaxError();
                return CMD_FAILURE;
            }
			
			string error;
			if(!graph->serialize(words[1].c_str(), error))
			{
				cliFailure("'%s'\n", error.c_str());
				return CMD_FAILURE;
			}

            cliSuccess("\n");
            
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccLoadGraph
// load graph from a dump file.
class ccLoadGraph: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + " FILENAME"; }
        string getHelpText()        { return _("load graph from a dump file."); }

        CommandStatus execute(vector<string> words, CoreCli *cli, BDigraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=2 || hasDataSet || inFile!=stdin )
            {
                syntaxError();
                return CMD_FAILURE;
            }

			string error;
			if(!graph->deserialize(words[1].c_str(), error))
			{
				cliFailure("'%s'\n", error.c_str());
				return CMD_FAILURE;
			}

            cliSuccess("\n");
            
            return CMD_SUCCESS;
        }
};







CoreCli::CoreCli(BDigraph *g): myGraph(g), doQuit(false)
{
#define CORECOMMANDS_BEGIN
#define CORECOMMANDS_END

#define CORECOMMAND(name, level, coreImp...)\
    ({                                  \
    CoreCliCommand *cmd= new coreImp;   \
    cmd->setName(name);                 \
    commands.push_back(cmd);            \
    })

#include "corecommands.h"
}



int main()
{
    setlocale(LC_ALL, "");
    bindtextdomain("graphcore", "./messages");
    textdomain("graphcore");

    BDigraph graph;
    CoreCli cli(&graph);

    cli.run();

    return 0;

}
