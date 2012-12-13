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

#ifndef DIGRAPH_H
#define DIGRAPH_H

#define DIGRAPH_FILEDUMP_ID	"GraphCoreDump-02"

#ifdef USE_MMAP_POOL
#include "mmap_pool.h"
template<typename T>
struct cdeque_map
{
    typedef std::deque<T,singlepage_std_allocator<T,QuickFixedsizeAllocator>> type;
};
#else
template<typename T>
struct cdeque_map
{
    typedef std::deque<T,std::allocator<T>> type;
};
#endif

// erase items in sorted range [start,end) which appear in sorted range [eraseStart,eraseEnd)
// returns iterator past the last item of the range
template <typename ITERATOR>
ITERATOR inplace_erase(ITERATOR start, ITERATOR end, ITERATOR eraseStart, ITERATOR eraseEnd)
{
    ITERATOR src= start,
             dest= start,
             eSrc= eraseStart;
    int found= 0;

    for(; src!=end; ++src)
    {
        if(eSrc!=eraseEnd && *src==*eSrc)
        {
            ++eSrc;
        }
        else
        {
            *dest= *src;
            ++dest;
        }
    }

    if(eSrc!=eraseEnd)
    {
        dprint(" *********** inplace_erase: skipped %d arcs\n", eraseEnd-eSrc);
//        DebugBreak();
    }

    return dest;
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

#ifdef USE_MMAP_POOL
//                 ptr, size in bytes
typedef std::unordered_map<void*, size_t> mmapmap_t;
static mmapmap_t& getMmapMap()
{
    static mmapmap_t *mmap_map;
    if(!mmap_map) mmap_map= new mmapmap_t;
    return *mmap_map;
}
static std::mutex& getMapMutex()
{
    static std::mutex m;
    return m;
}

namespace std
{
    template<>
    std::pair<BasicArc*, std::ptrdiff_t> get_temporary_buffer(std::ptrdiff_t count)
    {
        ScopedLock lock(getMapMutex());
        size_t len= count*sizeof(BasicArc);
        void *p= ::mmap(0, len, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
//        dmsg("count=%zu bytes=%zu p=%p pend=%p\n", count, len, p, (char*)p+len);
        if(p!=(void*)-1)
        {
            getMmapMap()[p]= len;
            return std::pair<BasicArc*, ptrdiff_t>((BasicArc*)p, count);
        }
        perror("mmap failed");
//        sleep(10);
        return std::pair<BasicArc*, ptrdiff_t>(0, 0);
    }

    template<>
    void return_temporary_buffer(BasicArc* p)
    {
        ScopedLock lock(getMapMutex());
        mmapmap_t::iterator it= getMmapMap().find(p);
        if(it==getMmapMap().end())
        {
            dmsg("%p not found\n", p);
            return;
        }
        size_t len= it->second;
//        dmsg("%p len=%zu\n", p, len);
        if(::munmap(p, len)<0)
            perror("munmap");
        getMmapMap().erase(it);
    }
};

#endif //USE_MMAP_POOL


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
        // META: <name> = <value>\n
		// tail, head\n
		// tail, head\n ...
		
		// dump the graph to a file.
		bool serialize(const MetaMap& metaVars, const char *filename, std::string& error)
		{
			FILE *f= fopen(filename, "w");
			if(!f) { error= strerror(errno); return false; }
			if(fprintf(f, "%s\n%u\n", DIGRAPH_FILEDUMP_ID, size())<0)
			{ error= strerror(errno); fclose(f); return false; }
			for(MetaMap::const_iterator it= metaVars.begin(); it!=metaVars.end(); ++it)
			{
				if(fprintf(f, "META: %s = %s\n", it->first.c_str(), it->second.c_str())<0)
				{ error= strerror(errno); fclose(f); return false; }
			}
			for(ArcContainerIterator it= arcsByHead.begin(); it!=arcsByHead.end(); ++it)
			{
				if(!it->serialize(f))
				{ error= strerror(errno); fclose(f); return false; }
			}
			fclose(f);
			return true;
		}
		
		// load graph from a dump file
		bool deserialize(MetaMap& metaVars, const char *filename, std::string& error)
		{
			FILE *f= fopen(filename, "r");
			if(!f) { error= strerror(errno); return false; }
			char id[100];
			if(fscanf(f, "%99s\n", id)!=1 || strcmp(id, DIGRAPH_FILEDUMP_ID)!=0)
			{ error= _("could not read matching file format id"); fclose(f); return false; }
			size_t newsize;
			if(fscanf(f, "%zu\n", &newsize)!=1)
			{ error= strerror(errno); fclose(f); return false; }
			
			metaVars.clear();
			char metaName[64], metaVal[64];
			while(fscanf(f, "META: %s = %s\n", metaName, metaVal)==2)
			{
				dprint("load meta: %s = %s\n", metaName, metaVal);
				metaVars[metaName]= metaVal;
			}
			
			clear();
			size_t i;
			arc a;
			for(i= 0; i<newsize; i++)
			{
				if(!a.deserialize(f))
				{ error= strerror(errno); fclose(f); resort(0,0); return false; }
				addArc(a, false);
			}
			resort(0,0);
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
            if(doSort) resort(size()-1, size()-1);
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
        void resort(uint32_t beginTail, uint32_t beginHead)
        {
			dprint("resort: old size %zu, tail %+zd, head %+zd\n", size(), size()-beginTail, size()-beginHead);
            volatile double tStart= getTime();
			threadedSort(beginTail, beginHead);
            sortedSize= size();
			dprint("resort: new size %zu, time %5.0fms\n", size(), (getTime()-tStart)*1000);
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
            while(!Q.empty())
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
            BFSnode(uint32_t _niveau, uint32_t pathNext_):
                niveau(_niveau), pathNext(pathNext_) { }
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
                while(it->tail==node && it!=arcsByTail.end()) ++it;
            }
        }

        void findLeaves(vector<uint32_t> &result)
        {
            ArcContainerIterator it= arcsByHead.begin();
            while(it!=arcsByHead.end())
            {
                uint32_t node= it->head;
                if(!hasDescendant(node)) result.push_back(node);
                while(it->head==node && it!=arcsByHead.end()) ++it;
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
		
		// todo: make private
		struct ArcIndices { uint32_t byHead; uint32_t byTail; };
		ArcIndices findArcIndices(const arc& value)
		{
			auto findArc= [&](ArcContainer& arcs, bool(*compFn)(arc,arc)) -> ssize_t
            {
                ArcContainerIterator it= lower_bound(arcs.begin(), arcs.end(), 
                    value, compFn);
                if( it!=arcs.end() && *it==value )
                    return it-arcs.begin();
                return -1;
            };
            
			ArcIndices ret;
            ret.byHead= findArc(arcsByHead, compByHead);
			ret.byTail= findArc(arcsByTail, compByTail);
            return ret;
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

			ArcIndices i= findArcIndices(value);
            
            if(i.byTail!=uint32_t(-1) && i.byHead!=uint32_t(-1))
            {
                // (todo merge into one queue)
                arcRemovalQueueBH.push_back(i.byHead);
                arcRemovalQueueBT.push_back(i.byTail);
            }
            else
            {
                if(i.byTail!=(uint32_t)-1 && i.byHead!=(uint32_t)-1)
                {
                    // arc found in one but not in both arrays. state is invalid.
                    dprint("BUG -- i.tail=%u i.head=%u\n", i.byTail, i.byHead);
                    *(int*)0= 0;       // crash
                }
                dprint("queueArcForRemoval: arc (%u->%u) not found\n", tail, head);
            }
            
            return true;
        }
        
        int removeQueuedArcs(size_t minResortTail= arc::NODE_MAX, size_t minResortHead= arc::NODE_MAX)
        {
            arc markVal(arc::NODE_MAX, arc::NODE_MAX);
            auto mark= [&](ArcContainer& arcs, deque<size_t>& q) -> size_t
            {
                size_t minIdx= arcs.size();
                while(q.size())
//				for(deque<size_t>::iterator it= q.begin(); it!=q.end(); ++it)
                {
                    size_t idx= q.front();
//					size_t idx= *it;
                    if(idx<minIdx) minIdx= idx;
                    arcs[idx]= markVal;
                    q.pop_front();
                }
                return minIdx;
            };
            
            size_t oldSize= size();
            // now mark all arcs queued for removal
            minResortHead= min(minResortHead, mark(arcsByHead, arcRemovalQueueBH));
            minResortTail= min(minResortTail, mark(arcsByTail, arcRemovalQueueBT));
            // sort, so that marked arcs end up at containers' ends
            // only sort values from the smallest index of all removed arcs
            resort(minResortTail, minResortHead);
            // resort also took care of removing all marked arcs because they are duplicates --
            // except the last one
            if(arcsByHead.size() && arcsByHead.back()==markVal) arcsByHead.pop_back();
            if(arcsByTail.size() && arcsByTail.back()==markVal) arcsByTail.pop_back();
            sortedSize= size();
            return oldSize-size();
        }
	

        // replace predecessors (successors=false) or descendants (successors=true) of a node
        bool replaceNeighborsOld(uint32_t node, vector<uint32_t>& newNeighbors, bool successors)
        {
			volatile double tStart= getTime();

            NeighborIterator it(*this);
            if(successors) it.startDescendants(node);
            else it.startPredecessors(node);
			
            // remove old neighbors of node
            while(!it.checkFinished())
            {
                queueArcForRemoval(it.getArc().tail, it.getArc().head);
                ++it;
            }
			
			volatile double tQueueRemove= getTime();
            
            removeQueuedArcs();
			
			volatile double tRemove= getTime();

            // add new neighbors and resort.
            vector<uint32_t>::iterator p;
            int oldSize= arcsByHead.size();
            for(p= newNeighbors.begin(); p!= newNeighbors.end(); ++p)
            {
                if(successors) addArc(node, *p, false);
                else addArc(*p, node, false);
            }
			
			volatile double tAdd= getTime();
			
            resort(oldSize);
            sortedSize= size();
			
			volatile double tResort= getTime();
			
			dprint("queue: %3.0fms remove: %3.0fms add: %3.0fms resort: %3.0fms overall: %3.0fms\n",
				   (tQueueRemove-tStart)*1000, (tRemove-tQueueRemove)*1000, (tAdd-tRemove)*1000, (tResort-tAdd)*1000, (tResort-tStart)*1000);
			
            return true;
            
            // todo:
            // replace neighbors in-place without removing first, appending and removing the rest as necessary
            // possible?
            // todo: 
            // manage sorter threads better
        }


        template<typename IT, typename DEQUELIKE>
        static void makeNeighbors(IT begin, IT end, DEQUELIKE& dest, uint32_t neighbor, bool makeSuccessors)
        {
            for(IT it= begin; it!=end; ++it)
            {
                dest.push_back( makeSuccessors? (arc { neighbor, *it }): (arc { *it, neighbor }) );
            }
        }
        
        template<typename DEQUELIKE>
        void eraseArcsInplace(DEQUELIKE arcs)
        {
            dprint("eraseArcsInplace %d\n", arcs.size());
            if(arcs.empty()) return;
            stable_sort(arcs.begin(), arcs.end(), compByTail);
            auto eraseBegin= lower_bound(arcsByTail.begin(), arcsByTail.end(), arcs.front(), compByTail);
            auto erasedT= inplace_erase(eraseBegin, arcsByTail.end(), arcs.begin(), arcs.end());
            
            stable_sort(arcs.begin(), arcs.end(), compByHead);
            eraseBegin= lower_bound(arcsByHead.begin(), arcsByHead.end(), arcs.front(), compByHead);
            auto erasedH= inplace_erase(eraseBegin, arcsByHead.end(), arcs.begin(), arcs.end());

            if(erasedT-arcsByTail.begin()!=erasedH-arcsByHead.begin())
            {
                dprint("erase: inplace_erase return values differ by tail/head\n");
                dprint("arcs.size(): %zu erasedT: %zu erasedH: %zu\n", arcs.size(), erasedT-arcsByTail.begin(), erasedH-arcsByHead.begin());
                for(auto it= arcs.begin(); it!=arcs.end(); it++)
                    dprint("    arc: %u, %u\n", it->tail, it->head);
                // 'repair' graph by adding fake arcs
                while(erasedT-arcsByTail.begin()!=erasedH-arcsByHead.begin())
                    (erasedT-arcsByTail.begin() < erasedH-arcsByHead.begin()? *erasedT++: *erasedH++)= arc {0xFFFFFFFF,0xFFFFFFFF};
                arcsByTail.erase(erasedT, arcsByTail.end());
                arcsByHead.erase(erasedH, arcsByHead.end());
                sortedSize= arcsByHead.size();
                DebugBreak();
            }
            arcsByTail.erase(erasedT, arcsByTail.end());
            arcsByHead.erase(erasedH, arcsByHead.end());
            
            sortedSize= arcsByHead.size();
        }

        template<typename VECTORLIKE>
        bool replaceNeighbors(uint32_t node, VECTORLIKE newNeighbors, bool successors)
        {
			volatile double tStart= getTime();

            // sort new neighbors and remove any duplicates
            stable_sort(newNeighbors.begin(), newNeighbors.end());
            for(unsigned i= 1; i<newNeighbors.size(); i++)
                if(newNeighbors[i]==newNeighbors[i-1])
                {
                    dprint("dup in newNeighbors: %d\n", newNeighbors[i]);
                    newNeighbors.erase(newNeighbors.begin()+i);
                    i--;
                }
            // find old neighbors for building diff
            vector<uint32_t> oldNeighbors;
            map<uint32_t,BFSnode> nodeInfo;
            doBFS2<findAll> (node, 0, 1, oldNeighbors, nodeInfo, (successors? DESCENDANTS: PREDECESSORS));
            if(oldNeighbors.size()) oldNeighbors.erase(oldNeighbors.begin());   // remove the node itself.
            stable_sort(oldNeighbors.begin(), oldNeighbors.end());
            // build diff:
            uint32_t diffbuf[ oldNeighbors.size()+newNeighbors.size() ];
            // [diffbuf+0, idx_added): newly added neighbors
            auto idx_added= set_difference(newNeighbors.begin(),newNeighbors.end(), oldNeighbors.begin(),oldNeighbors.end(), diffbuf+0);
            // [idx_added, idx_removed): removed neighbors
            auto idx_removed= set_difference(oldNeighbors.begin(),oldNeighbors.end(), newNeighbors.begin(),newNeighbors.end(), idx_added);
            
            volatile double tBeforeRemove= getTime();

            if(idx_removed-idx_added)
            {
                deque<arc> erasedNeighbors;
                makeNeighbors(idx_added, idx_removed, erasedNeighbors, node, successors);
                eraseArcsInplace(erasedNeighbors);
            }
            volatile double tBeforeAdd= getTime();
            if(idx_added-diffbuf)
            {
                size_t sizeBefore= size();
                if(successors)
                    // add successors
                    for(auto it= diffbuf+0; it!=idx_added; ++it)
                        addArc(node, *it, false);
                else
                    // add predecessors
                    for(auto it= diffbuf+0; it!=idx_added; ++it)
                        addArc(*it, node, false);
                // merge in new neighbors
                resort(sizeBefore, sizeBefore);
                return true;
            }
            
            volatile double tEnd= getTime(); 
            
            if(idx_removed-idx_added)
                dprint("replaceNeighbors times: %zu added, %zu removed, remove: %3ums, add: %3ums, overall %3ums\n", 
                       idx_added-diffbuf, idx_removed-idx_added, 
                       unsigned((tBeforeAdd-tBeforeRemove)*1000), 
                       unsigned((tEnd-tBeforeAdd)*1000), unsigned((tEnd-tStart)*1000));
            
            return true;
        }

        // replace predecessors (successors=false) or descendants (successors=true) of a node
        // overwrite + resort method
        template<typename VECTORLIKE>
        bool replaceNeighborsOverwrite(uint32_t node, VECTORLIKE newNeighbors, bool successors)
        {
            NeighborIterator it(*this);
            if(successors) it.startDescendants(node);
            else it.startPredecessors(node);
            
            deque<ArcIndices> oldIndices;
			
            while(!it.checkFinished())
            {
                //~ queueArcForRemoval(it.getArc().tail, it.getArc().head);
                // todo: get internal index from NeighborIterator
				ArcIndices ind= findArcIndices(it.getArc());
				if(ind.byHead==BasicArc::NODE_MAX || ind.byTail==BasicArc::NODE_MAX)
					dprint("arc (%d,%d): byHead=%d, byTail=%d!\n", it.getArc().tail,it.getArc().head, ind.byHead,ind.byTail);
                oldIndices.push_back(ind);
                ++it;
            }
			
			dprint("oldIndices size: %d\nnewNeighbors size: %d\n", oldIndices.size(), newNeighbors.size());

			uint32_t minSortIdxTail= BasicArc::NODE_MAX;
			uint32_t minSortIdxHead= BasicArc::NODE_MAX;
            
			// replace common neighbors
            // while oldIndices left and newNeighbors left:
            //  replace oldIndices[idx] with newNeighbors[idx]
            //  idx++
			typename deque<ArcIndices>::iterator i_oldind= oldIndices.begin();
			vector<uint32_t>::iterator i_neighbor= newNeighbors.begin();
			for(; 
				i_oldind!=oldIndices.end() && i_neighbor!=newNeighbors.end(); 
				++i_oldind, ++i_neighbor)
			{
				volatile ArcIndices ind= *i_oldind;
				uint32_t newNeighbor= *i_neighbor;
                // xxx combine this?
				if(successors)
				{
                    if(arcsByHead[ind.byHead].head != newNeighbor)
                    {
                        arcsByHead[ind.byHead].head= newNeighbor;
                        if(ind.byHead<minSortIdxHead) minSortIdxHead= ind.byHead;
                    }
                    if(arcsByTail[ind.byTail].head != newNeighbor)
                    {
                        arcsByTail[ind.byTail].head= newNeighbor;
                        if(ind.byTail<minSortIdxTail) minSortIdxTail= ind.byTail;
                    }
				}
				else
				{
                    if(arcsByHead[ind.byHead].tail != newNeighbor)
                    {
                        arcsByHead[ind.byHead].tail= newNeighbor;
                        if(ind.byHead<minSortIdxHead) minSortIdxHead= ind.byHead;
                    }
					if(arcsByTail[ind.byTail].tail != newNeighbor)
                    {
                        arcsByTail[ind.byTail].tail= newNeighbor;
                        if(ind.byTail<minSortIdxTail) minSortIdxTail= ind.byTail;
                    }
				}
			}
			
            // case: more new neighbors than old
            // while newNeighbors left:
            //  append newNeighbors[i]
            //  i++
			for(; i_neighbor!=newNeighbors.end(); ++i_neighbor)
			{
				arc newArc;
				if(successors)
					newArc.tail= node,
					newArc.head= *i_neighbor;
				else
					newArc.tail= *i_neighbor,
					newArc.head= node;
				addArc(newArc, false);
			}
            
            // case: less new neighbors than old
            // while oldIndices left:
            //  queueArcForRemoval(oldIndices[idx])
			//  idx++
			dprint("i_oldind-begin: %zd -- end-i_oldind: %zd\n", 
				i_oldind-oldIndices.begin(), oldIndices.end()-i_oldind);
			for(; i_oldind!=oldIndices.end(); ++i_oldind)
			{
				arcRemovalQueueBH.push_back(i_oldind->byHead);
				arcRemovalQueueBT.push_back(i_oldind->byTail);
			}
			
			removeQueuedArcs(minSortIdxTail, minSortIdxHead);
			
			return true;
        }


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
#ifdef STATS_AVGNEIGHBORS // calculate average successors/predecessors per node. this takes a little long.
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
#ifdef STATS_AVGNEIGHBORS
                totalPredecessors[h.head]++;
                totalSuccessors[h.tail]++;
#endif
            }

#ifdef STATS_AVGNEIGHBORS
            size_t s= 0;
            for(map<uint32_t,uint32_t>::iterator it= totalPredecessors.begin(); it!=totalPredecessors.end(); ++it)
                s+= it->second;
            if(s) s/= totalPredecessors.size();
            result["AvgPredecessors"]= statInfo(_("average predecessors per node"), s);
            s= 0;
            for(map<uint32_t,uint32_t>::iterator it= totalSuccessors.begin(); it!=totalSuccessors.end(); ++it)
                s+= it->second;
            if(s) s/= totalSuccessors.size();
            result["AvgSuccessors"]= statInfo(_("average successors per node"), s);
#endif

            result["MinNodeID"]= statInfo(_("lowest node ID"), minNodeID==U32MAX? 0: minNodeID);
            result["MaxNodeID"]= statInfo(_("greatest node ID"), maxNodeID);
            result["NumDups"]= statInfo(_("number of duplicates found (must be zero)"), numDups);
            result["DataInvalid"]= statInfo(_("nonzero if any obvious errors were found in graph data"), invalid);
			result["ContainerFragmentsHead"]= statInfo(_("container fragments (head)"), countContainerFragments(arcsByHead.begin(), arcsByHead.end()));
			result["ContainerFragmentsTail"]= statInfo(_("container fragments (tail)"), countContainerFragments(arcsByTail.begin(), arcsByTail.end()));
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
//        typedef deque< arc > ArcContainer;
        typedef typename cdeque_map<arc>::type ArcContainer;
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

		// count non-contiguous blocks in arc container
		uint32_t countContainerFragments(ArcContainerIterator begin, ArcContainerIterator end)
		{
			uint32_t frags= 0;
			for(ArcContainerIterator it= begin; it!=end; ++it)
			{
				if( size_t( &(*it)- &(*(it-1)) ) != 1 )
					frags++; 
			}
			return frags;
		}
        
		
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
        void threadedSort(uint32_t mergeBeginTail, uint32_t mergeBeginHead)
        {
            pthread_t threadID;
            sorterThreadArg arg= { arcsByHead, 0, mergeBeginHead, arcsByHead.size(), compByHead };
            pthread_create(&threadID, 0, sorterThread, &arg);
            doMerge(arcsByTail, 0, mergeBeginTail, arcsByTail.size(), compByTail);
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
			dprint("doMerge begin=%d mergeBegin=%d end=%d\n", begin, mergeBegin, end);
			volatile double tStart= getTime();
            std::stable_sort(arcs.begin()+mergeBegin, arcs.begin()+end, compFunc);
			volatile double tSort1= getTime();

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
                dprint("%u dups in merge set, begin=%d mergeBegin=%d end=%d size()=%zu\n", 
                    numDups, begin, mergeBegin, end, arcs.size());
            }

			volatile double tEraseDups= getTime();
            
            auto minBegin= lower_bound(arcs.begin(), arcs.begin()+mergeBegin, arcs[mergeBegin], compFunc);
            
            dprint("doMerge: minBegin=%u\n", minBegin-arcs.begin());

            inplace_merge(/*arcs.begin()+begin*/minBegin, arcs.begin()+mergeBegin, arcs.begin()+end, compFunc);
			volatile double tEnd= getTime();

//			dprint("doMerge: sort mergeBegin=>end   t=%5.0fms\n", (tSort1-tStart)*1000);
//			dprint("doMerge: erase dups:            t=%5.0fms\n", (tEraseDups-tSort1)*1000);
//			dprint("doMerge: inplace_merge:         t=%5.0fms\n", (tEnd-tEraseDups)*1000);
        }
        
        friend class ccRMStuff; // can read arc data directly for debugging.
};
typedef Digraph<BasicArc> BDigraph;

#endif //DIGRAPH_H


