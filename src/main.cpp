// Graph Processor core.
// (c) Wikimedia Deutschland, written by Johannes Kroll in 2011

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


class Digraph
{
    public:
        struct arc
        {
            uint32_t tail, head;

            arc(): tail(0), head(0) {}
            arc(uint32_t _tail, uint32_t _head): tail(_tail), head(_head) {}
            bool operator< (arc a) const
            {
                return (a.tail==tail? a.head<head: a.tail<tail);
            }
            bool operator== (arc a) const
            {
                return (a.tail==tail && a.head==head);
            }
        };

        Digraph()
        {
        }

        ~Digraph()
        {
        }


        // add an arc to the graph
        void addArc(arc a, bool doSort= true)
        {
            arcContainer::iterator lb= lower_bound(arcsByHead.begin(), arcsByHead.end(), a, compByHead);
            if(lb!=arcsByHead.end() && *lb==a) { return; }
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
        }

        // re-sort arcs starting with given index
        void resort(uint32_t begin= 0)
        {
            threadedSort(begin);
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
            arcContainer::iterator it= findArcByHead(node);
            return (it!=arcsByHead.end() && it->head==node);
        }

        // does this node have any descendants?
        bool hasDescendant(uint32_t node)
        {
            arcContainer::iterator it= findArcByTail(node);
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
            arcContainer::iterator it= arcsByTail.begin();
            while(it!=arcsByTail.end())
            {
                uint32_t node= it->tail;
                if(!hasPredecessor(node)) result.push_back(node);
                while(it->tail==node && it!=arcsByTail.end()) it++;
            }
        }

        void findLeaves(vector<uint32_t> &result)
        {
            arcContainer::iterator it= arcsByHead.begin();
            while(it!=arcsByHead.end())
            {
                uint32_t node= it->head;
                if(!hasDescendant(node)) result.push_back(node);
                while(it->head==node && it!=arcsByHead.end()) it++;
            }
        }


        // erase an arc from the graph
        void eraseArc(uint32_t tail, uint32_t head)
        {
            arcContainer::iterator it;
            arc value= arc(tail, head);
            if( (it= lower_bound(arcsByHead.begin(), arcsByHead.end(), value, compByHead))!=arcsByHead.end() &&
                    *it==value )
                arcsByHead.erase(it);
            if( (it= lower_bound(arcsByTail.begin(), arcsByTail.end(), value, compByTail))!=arcsByTail.end() &&
                    *it==value )
                arcsByTail.erase(it);
        }

        // replace predecessors (successors=false) or descendants (successors=true) of a node
        bool replaceNeighbors(uint32_t node, vector<uint32_t> newNeighbors, bool successors)
        {
            NeighborIterator it(*this);
            if(successors) it.startDescendants(node);
            else it.startPredecessors(node);
            stable_sort(newNeighbors.begin(), newNeighbors.end());
            vector<uint32_t>::iterator p= newNeighbors.begin();
            // replace arcs
            for(; !it.finished() && p!=newNeighbors.end(); ++it, ++p)
                (successors? it.getArc().head: it.getArc().tail)= *p;
            if(!it.finished())
            {
                // remove the rest
                for(; !it.checkFinished(); )
                {
                    arcContainer::iterator f=
                        (successors? lower_bound(arcsByHead.begin(), arcsByHead.end(), it.getArc(), compByHead):
                         lower_bound(arcsByTail.begin(), arcsByTail.end(), it.getArc(), compByTail));
                    if(f==(successors? arcsByHead.end(): arcsByTail.end()) || !(*f==it.getArc()))
                        return false;
                    if(successors)
                        arcsByTail.erase(it.getIterator()),
                        arcsByHead.erase(f);
                    else
                        arcsByTail.erase(f),
                        arcsByHead.erase(it.getIterator());
                }
            }
            else if(p!=newNeighbors.end())
            {
                // add new ones and resort
                int oldSize= arcsByHead.size();
                for(; p!=newNeighbors.end(); p++)
                    if(successors) addArc(node, *p, false);
                    else addArc(*p, node, false);
                resort(oldSize);
            }
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
            if(size!=arcsByTail.size()) invalid= true;
            uint32_t numDups= 0;
            uint32_t minNodeID= U32MAX, maxNodeID= 0;
//#define AVGNEIGHBORS
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


        void sizeSanityCheck()
        {
            // just to show that there is no padding for 32-bit values. even on 64-bit platforms.
            printf("distance between arcs in bytes (should be 8): %d\n",
                   (char*)&arcsByHead[1] - (char*)&arcsByHead[0]);
        }


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
                fprintf(outFile, "%d -> %d\n", arcsByHead[i].tail, arcsByHead[i].head);
        }

        // list arcs by index sorted by tail
        void listArcsByTail(uint32_t start, uint32_t end, FILE *outFile= stdout)
        {
            for(uint32_t i= start; i<end && i<arcsByTail.size(); i++)
                fprintf(outFile, "%d -> %d\n", arcsByTail[i].tail, arcsByTail[i].head);
        }


    protected:
        typedef deque< arc > arcContainer;
//        typedef vector< arc > arcContainer;
        arcContainer arcsByTail, arcsByHead;

        // helper class for iterating over all predecessors/successors (or both) of a node
        class NeighborIterator
        {
            private:
                arcContainer::iterator it;
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

                arcContainer::iterator getIterator()
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
        arcContainer::iterator findArcByHead(uint32_t head)
        {
            arc value(0, head);
            return lower_bound(arcsByHead.begin(), arcsByHead.end(), value, compByHead);
        }

        // find the position of first arc with given tail (lower bound)
        arcContainer::iterator findArcByTail(uint32_t tail)
        {
            arc value(tail, 0);
            return lower_bound(arcsByTail.begin(), arcsByTail.end(), value, compByTail);
        }

        struct sorterThreadArg
        {
            arcContainer::iterator begin, mergeBegin, end;
            bool (*compFunc)(arc a, arc b);
        };
        // thread function for sorting
        static void *sorterThread(void *a)
        {
            sorterThreadArg *arg= (sorterThreadArg*)a;
            doMerge(arg->begin, arg->mergeBegin, arg->end, arg->compFunc);
            return 0;
        }
        // re-sort arcs in 2 threads
        void threadedSort(int mergeBegin)
        {
            pthread_t threadID;
            sorterThreadArg arg= { arcsByHead.begin(), arcsByHead.begin()+mergeBegin, arcsByHead.end(), compByHead };
            pthread_create(&threadID, 0, sorterThread, &arg);
            doMerge(arcsByTail.begin(), arcsByTail.begin()+mergeBegin, arcsByTail.end(), compByTail);
            pthread_join(threadID, 0);
        }

        // helper function: merge & resort
        static void doMerge(arcContainer::iterator begin, arcContainer::iterator mergeBegin, arcContainer::iterator end,
                            bool (*compFunc)(arc a, arc b))
        {
            stable_sort(mergeBegin, end, compFunc);
            inplace_merge(begin, mergeBegin, end, compFunc);
        }
};




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
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)= 0;
};

// cli commands which return a node list data set.
class CliCommand_RTNodeList: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_NODE_LIST; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                             vector<uint32_t> &result)= 0;
};

// cli commands which return an arc list data set.
class CliCommand_RTArcList: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_ARC_LIST; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                                      vector<Digraph::arc> &result)= 0;
};

// cli commands which return some other data set. execute() must print the result to outFile.
class CliCommand_RTOther: public CoreCliCommand
{
    public:
        ReturnType getReturnType() { return RT_OTHER; }
        virtual CommandStatus execute(vector<string> words, class CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)= 0;
};



class CoreCli: public Cli
{
    public:
        CoreCli(Digraph *g);

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
                d= strchr(command, ':');
                if(d)
                    *d= 0, commandHasDataSet= true;
                else
                    commandHasDataSet= false;
                if(strlen(command))
                    execute(command, commandHasDataSet, inRedir, outRedir),
                    add_history(completeCommand);
                free(command);
                free(completeCommand);
            }
        }

        void quit() { doQuit= true; }



    protected:
        Digraph *myGraph;

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
                CommandStatus status;
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
// && should return NONE *only* if one of the operants return NONE.
// &&! should return NONE if the left operant is NONE. NONE on the right side is treated like an empty set.
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
                        vector<Digraph::arc> result;
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
};



///////////////////////////////////////////////////////////////////////////////////////////
// ccListNeighbors
// template class for list-* commands
template<Digraph::NodeRelation searchType, bool recursive>
    class ccListNeighbors: public CliCommand_RTNodeList
{
    public:
        string getSynopsis()        { return getName() + _(" NODE") + (recursive? _(" DEPTH"): ""); }
        string getHelpText()
        {
            if(recursive) switch(searchType)
            {
                case Digraph::NEIGHBORS: return _("list NODE and its neighbors recursively up to DEPTH.");
                case Digraph::PREDECESSORS: return _("list NODE and its predecessors recursively up to DEPTH.");
                case Digraph::DESCENDANTS: return _("list NODE and its successors recursively up to DEPTH.");
            }
            else switch(searchType)
            {
                case Digraph::NEIGHBORS: return _("list direct neighbors of NODE.");
                case Digraph::PREDECESSORS: return _("list direct predecessors of NODE.");
                case Digraph::DESCENDANTS: return _("list direct successors of NODE.");
            }
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
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
            map<uint32_t,Digraph::BFSnode> nodeInfo;
            graph->doBFS2<Digraph::findAll> (Cli::parseUint(words[1]), 0, (recursive? Cli::parseUint(words[2]): 1),
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
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
# This way, a composite command is formed. Note that if either operant\n\
# fails, the composite command also fails.\n\
# \n\
# The following operators are currently specified:\n\
# \n\
# intersection (&&):\n\
# The intersection operator takes two operants, both of wich must\n\
# return a set of nodes. The result of the composite command is a set of\n\
# nodes that contains only the nodes that are in both, the result of the\n\
# left operand, and the result of the right. If and only if either\n\
# operant returns NONE, the result is NONE. \n\
# \n\
# subtraction (&&!):\n\
# The subtraction operator takes two operants, both of\n\
# which must return a set of nodes. The result of the composite command is\n\
# a set of nodes that contains only the nodes that are in the result of\n\
# the left operand but not in the result of the right operant. If and only\n\
# if the left operant returns NONE, the result is NONE. If the right\n\
# operant returns NONE, the result is the result of the left operant.\n\n");
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
                cliSuccess(_("available commands%s\n"), outFile==stdout? ":": "");
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
            Digraph graph;
            map<string, Digraph::statInfo> info;
            graph.getStats(info);
            for(map<string, Digraph::statInfo>::iterator i= info.begin(); i!=info.end(); i++)
                s+= "\n# " + i->first + "\t" + i->second.description;
            return s;
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if(words.size()!=1 || hasDataSet)
            {
                syntaxError();
                return CMD_FAILURE;
            }
            map<string, Digraph::statInfo> info;
            graph->getStats(info);
            cliSuccess("%s\n", outFile==stdout? ":": "");
            cout << lastStatusMessage;
            for(map<string, Digraph::statInfo>::iterator i= info.begin(); i!=info.end(); i++)
                fprintf(outFile, "%s,%zu\n", i->first.c_str(), i->second.value);
            fprintf(outFile, "\n");
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
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
            for(unsigned lineno= 1; ; lineno++)
            {
                record.clear();
                if( !Cli::readNodeIDRecord(inFile, record) )
                {
                    if(ok) cliError(_("error reading data set (line %u)\n"), lineno);
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
                    if(ok) cliError(_("error reading data set (line %u)\n"), lineno);
                    ok= false;
                }
                else
                {
                    if(record[0]==0 || record[1]==0) { cliError(_("invalid node ID in line %d\n"), lineno); ok= false; }
                    if(ok) graph->addArc(record[0], record[1], false);
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=1 || !(hasDataSet||(inFile!=stdin)) )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            vector< vector<uint32_t> > dataset;
            if(!readNodeset(inFile, dataset, 2))
                return CMD_FAILURE;

            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                graph->eraseArc((*i)[0], (*i)[1]);

            cliSuccess("\n");
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccReplaceNeighbors
// replace-predecessors/replace-successors commands
template<Digraph::NodeRelation searchType>
    class ccReplaceNeighbors: public CliCommand_RTVoid
{
    public:
        string getSynopsis()        { return getName() + _(" NODE") + " {:|<}"; }
        string getHelpText()
        {
            switch(searchType)
            {
                case Digraph::PREDECESSORS: return _("read data set of nodes and replace predecessors of NODE with given set.");
                case Digraph::DESCENDANTS: return _("read data set of nodes and replace successors of NODE with given set.");
                default:    ;
            }
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
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

            if(graph->replaceNeighbors(Cli::parseUint(words[1]), newNeighbors, searchType==Digraph::DESCENDANTS))
            {
                cliSuccess("\n");
                return CMD_SUCCESS;
            }
            else
            {
                cliError(_("internal error: Digraph::replaceNeighbors() failed.\n"));
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile, vector<Digraph::arc> &result)
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
            map<uint32_t,Digraph::BFSnode> nodeInfo;
            uint32_t node;

            if(findRoot)
                node= graph->doBFS2<Digraph::findRoot> (Cli::parseUint(words[1]), 0, U32MAX,
                                                        nodes, nodeInfo);
            else
                node= graph->doBFS2<Digraph::findNode> (Cli::parseUint(words[2]), Cli::parseUint(words[1]), U32MAX,
                                                        nodes, nodeInfo);
            if(node)
            {
                uint32_t next;
                result.resize(nodeInfo[node].niveau);
                vector<Digraph::arc>::iterator it= result.begin();
                while((next= nodeInfo[node].pathNext))
                {
                    *it++= Digraph::arc(node, next);
                    node= next;
                }
                cliSuccess(_("%zu nodes visited, path length %zu%s\n"), nodes.size(), result.size(),
                           outFile==stdout? ":": "");
                return CMD_SUCCESS;
            }
//            lastStatusMessage= NONE_STR "\n";
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if( hasDataSet ||
                (words.size()==3 && !Cli::isValidUint(words[2])) ||
                (words.size()<2 || words.size()>3) ||
                !Cli::isValidUint(words[1]) )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            // no i/o redirection for this debug command.
            puts(SUCCESS_STR " :");

            uint32_t start= Cli::parseUint(words[1]),
                     end= (words.size()==3? start+Cli::parseUint(words[2]): graph->size());
            if(byHead) graph->listArcsByHead(start, end);
            else graph->listArcsByTail(start, end);

            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccAddStuff
// (debugging)
class ccAddStuff: public CliCommand_RTOther
{
    public:
        string getSynopsis()        { return getName(); }
        string getHelpText()
        {
            return _("debugging");
        }

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
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

//            vector< vector<uint32_t> > tmp;
//            tmp.resize(num);

            uint32_t oldSize= graph->size();
            for(unsigned i= 0; i<num; i++)
            {
                graph->addArc(rand()%10000, rand()%10000, false);
            }
            graph->resort(oldSize);

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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if( hasDataSet || words.size()!=1 )
            {
                syntaxError();
                return CMD_FAILURE;
            }

            graph->sizeSanityCheck();
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

        CommandStatus execute(vector<string> words, CoreCli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
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




CoreCli::CoreCli(Digraph *g): myGraph(g), doQuit(false)
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

    Digraph graph;
    CoreCli cli(&graph);

    cli.run();

    return 0;

}
