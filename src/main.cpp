// Graph Processor core.
// (c) Wikimedia Deutschland, written by Johannes Kroll in 2011
#include <cstring>
#include <cstdarg>
#include <cstdio>
#include <iostream>
#include <vector>
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

using namespace std;


#define _(string) gettext(string)

#define U32MAX (0xFFFFFFFF)


// time measurement
double getTime()
{
    timeval tv;
    gettimeofday(&tv, 0);
    return tv.tv_sec + tv.tv_usec*0.000001;
}

// test if stdout refers to tty
bool isInteractive()
{
    return isatty(STDOUT_FILENO) || isatty(STDIN_FILENO);
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

        // is this a node with no predecessors?
        bool isRoot(uint32_t node)
        {
            arcContainer::iterator it= findArcByHead(node);
            return !(it==arcsByHead.end() || it->head==node);
        }

        // is this a node with no descendant?
        bool isLeaf(uint32_t node)
        {
            arcContainer::iterator it= findArcByTail(node);
            return !(it==arcsByTail.end() || it->tail==node);
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
            { return graph.isRoot(node); }
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
                if(isRoot(node)) result.push_back(node);
                while(it->tail==node && it!=arcsByTail.end()) it++;
            }
        }

        void findLeaves(vector<uint32_t> &result)
        {
            arcContainer::iterator it= arcsByHead.begin();
            while(it!=arcsByHead.end())
            {
                uint32_t node= it->head;
                if(isLeaf(node)) result.push_back(node);
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
            for(int i= 0; i<size; i++)
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
        void listArcsByHead(uint32_t start, uint32_t end)
        {
            for(uint32_t i= start; i<end && i<arcsByHead.size(); i++)
                printf("%d -> %d\n", arcsByHead[i].tail, arcsByHead[i].head);
        }

        // list arcs by index sorted by tail
        void listArcsByTail(uint32_t start, uint32_t end)
        {
            for(uint32_t i= start; i<end && i<arcsByTail.size(); i++)
                printf("%d -> %d\n", arcsByTail[i].tail, arcsByTail[i].head);
        }


    protected:
        typedef vector<arc> arcContainer;
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


// convenience macros for printing success/failure/error messages from the cli.
// if the protocol for those messages should ever have to be modified, change these.
#define SUCCESS_STR "OK."
#define FAIL_STR "FAILED!"
#define ERROR_STR "ERROR!"
#define NONE_STR "NONE."
#define cliMessage(str, x...) ({ char c[2048]; int n= sprintf(c, str " "); snprintf(c+n, sizeof(c)-n, x); lastStatusMessage= c; })
#define cliSuccess(x...) cliMessage(SUCCESS_STR, x)
#define cliFailure(x...) cliMessage(FAIL_STR, x)
#define cliError(x...) cliMessage(ERROR_STR, x)
#define cliNone(x...) cliMessage(NONE_STR, x)

enum CommandStatus
{
    CMD_SUCCESS= 0,
    CMD_FAILURE,
    CMD_ERROR,
    CMD_NONE,
};

// abstract base class for cli commands
class CliCommand
{
    public:
        enum ReturnType
        {
            RT_NONE,
            RT_ARC_LIST,
            RT_NODE_LIST,
            RT_OTHER,
        };

        CliCommand() { }
        virtual ~CliCommand() { }

        // the command name
        virtual string getName()            { return "CliCommand"; }
        // one line describing the command and its parameters
        virtual string getSynopsis()        { return getName(); }
        // help text describing the function of the command
        virtual string getHelpText()        { return "Help text for " + getName() + "."; }
        void syntaxError()                  { lastStatusMessage= string(FAIL_STR) + _(" Syntax: ") + getSynopsis() + "\n"; }
        const string &getStatusMessage()    { return lastStatusMessage; }
        virtual ReturnType getReturnType()= 0;

        // read a data set of node IDs.
        // expectedSize: expected size of set per line (e. g. 1 for nodes, 2 for arcs)
        // update lastErrorString and return true on success, false on failure.
        bool readNodeset(FILE *inFile, vector< vector<uint32_t> > &dataset, unsigned expectedSize);

    protected:
        string lastStatusMessage;
};


// base classes for cli commands. derive commands from these.
// YourCliCommand::execute() must return the appropriate CommandStatus error code.

// cli commands which do not return a data set.
class CliCommand_RTVoid: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_NONE; }
        virtual CommandStatus execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)= 0;
};

// cli commands which return a node list data set.
class CliCommand_RTNodeList: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_NODE_LIST; }
        virtual CommandStatus execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                             vector<uint32_t> &result)= 0;
};

// cli commands which return an arc list data set.
class CliCommand_RTArcList: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_ARC_LIST; }
        virtual CommandStatus execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                                      vector<Digraph::arc> &result)= 0;
};

// cli commands which return some other data set. execute() must print the result to outFile.
class CliCommand_RTOther: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_OTHER; }
        virtual CommandStatus execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)= 0;
};



class Cli
{
    public:
        Cli(Digraph *g);

        ~Cli()
        {
            for(unsigned i= 0; i<commands.size(); i++)
                delete(commands[i]);
            commands.clear();
        }

        CliCommand *findCommand(string name)
        {
            for(unsigned i= 0; i<commands.size(); i++)
                if(commands[i]->getName()==name) return commands[i];
            return 0;
        }

        vector<CliCommand*> &getCommands() { return commands; }

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

        // convert string to unsigned int
        static uint32_t parseUint(string str)
        {
            return strtoul(str.c_str(), 0, 0);
        }

        // check if string forms a valid unsigned integer
        static bool isValidUint(const string& s)
        {
            // allow only positive decimal digits
            for(int i= 0; s[i]; i++)
                if( !isdigit(s[i]) ) return false;
            return true;
        }

        // check if string forms a valid node (vertex) id.
        static bool isValidNodeID(const string& s)
        {
            return isValidUint(s) && parseUint(s)!=0;
        }

        // parse a data record in text form, check for valid uints
        static bool readUintRecord(FILE *f, vector<uint32_t> &ret)
        {
            char line[1024];
            uint32_t n;
            if(fgets(line, 1024, f)==0)
            {
                if(!feof(f)) return false;
                else return true;
            }
            if( (n= strlen(line)) && line[n-1]=='\n' ) line[--n]= 0;
            vector<string> strings= splitString(line);
            for(uint32_t i= 0; i<strings.size(); i++)
            {
                if(!isValidUint(strings[i])) return false;
                ret.push_back(parseUint(strings[i]));
            }
            return true;
        }

        // like readUintRecord(), and also check that integers are in valid range for node IDs (currently 1..uint32_max)
        static bool readNodeIDRecord(FILE *f, vector<uint32_t> &ret)
        {
            if(!readUintRecord(f, ret)) return false;
            for(vector<uint32_t>::iterator i= ret.begin(); i!=ret.end(); i++)
                if(*i==0) return false;
            return true;
        }


    protected:
        Digraph *myGraph;
        bool doQuit;

        vector<CliCommand*> commands;

        // read a line from stdin using readline if appropriate
        // return 0 on error
        char *getLine()
        {
            if(isInteractive()) return readline("> ");
            size_t n;
            char *lineptr= 0;
            ssize_t s= getline(&lineptr, &n, stdin);
            if(s<0) return 0;
            return lineptr;
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

            vector<string>::iterator op= find(words.begin(), words.end(), "&&");
            string opstring;
            vector<string> words2;
            if(op==words.end()) op= find(words.begin(), words.end(), "&&!");
            if(op!=words.end())
            {
                opstring= *op;
                words.erase(op);
                while(op!=words.end())
                    words2.push_back(*op), words.erase(op);
            }

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
                                    if(end!=mergeResult.begin())
                                    {
                                        cout << SUCCESS_STR <<
                                            " L: " << result.size() << " R: " << result2.size() << " -> " << end-mergeResult.begin() <<
                                            (outRedir? "": ":") << endl;
                                        for(vector<uint32_t>::iterator it= mergeResult.begin(); it!=end; it++)
                                            fprintf(outFile, "%u\n", *it);
                                        fprintf(outFile, "\n");
                                    }
                                    else
                                        cout << "NONE." << endl;
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

        // get i/o redirection filename from command line
        static char *getRedirFilename(char *str)
        {
            while(isspace(*str)) str++;
            char *s= str;
            while(*s && !isspace(*s)) s++;
            *s= 0;
            return str;
        }

        // split a string into words using given delimiters
        static vector<string> splitString(char *str, const char *delim= " \n\t,")
        {
            vector<string> ret;
            while(true)
            {
                while(*str && strchr(delim, *str)) str++;
                if(!*str) return ret;
                char *start= str;
                while(*str && !strchr(delim, *str)) str++;
                if(*str) *str++= 0;
                ret.push_back(string(start));
            }
        }
};


// read a data set of node IDs.
// expectedSize: expected size of set per line (e. g. 1 for nodes, 2 for arcs)
// update lastErrorString and return true on success, false on failure.
bool CliCommand::readNodeset(FILE *inFile, vector< vector<uint32_t> > &dataset, unsigned expectedSize)
{
    vector<uint32_t> record;
    bool ok= true;
    cliSuccess("\n");
    for(unsigned lineno= 1; ; lineno++)
    {
        record.clear();
        if( !Cli::readNodeIDRecord(inFile, record) )
        {
            cliError(_("error reading data set (line %u)\n"), lineno);
            ok= false;
        }
        else if(record.size()==0)
        {
            return ok;
        }
        else if(record.size()!=expectedSize)
        {
            cliError(_("error reading data set (line %u)\n"), lineno);
            ok= false;
        }
        else
        {
            if(record[0]==0 || record[1]==0) { cliError(_("invalid node ID in line %d\n"), lineno); ok= false; }
            if(ok) dataset.push_back(record);
        }
    }
}


///////////////////////////////////////////////////////////////////////////////////////////
// ccListNeighbors
// template class for list-* commands
template<Digraph::NodeRelation searchType, bool recursive>
    class ccListNeighbors: public CliCommand_RTNodeList
{
    private:
        string name;

    public:
        ccListNeighbors(const char *_name): name(_name)
        { }

        string getName()            { return name; }
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

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
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
    private:
        string name;

    public:
        ccListNeighborless(const char *_name): name(_name)
        { }

        string getName()            { return name; }
        string getSynopsis()        { return getName(); }
        string getHelpText()
        {
            if(leaves)
                return _("list leaf nodes (nodes without successors).");
            else
                return _("list root nodes (nodes without predecessors).");
        }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                     vector<uint32_t> &result)
        {
            if( words.size()!=1 || hasDataSet )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            if(leaves) graph->findLeaves(result);
            else graph->findRoots(result);
            cliSuccess(_("%zu %s%s\n"), result.size(), (leaves? _("leaf nodes"): _("root nodes")),
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
        Cli *cli;

    public:
        ccHelp(Cli *_cli): cli(_cli)
        { }

        string getName()            { return "help"; }
        string getSynopsis()        { return getName() + _(" [COMMAND]"); }
        string getHelpText()        { return _("get help on COMMAND/list commands."); }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if(words.size()>2 || hasDataSet)
            {
                syntaxError();
                return CMD_FAILURE;
            }
            if(words.size()==2)
            {
                CliCommand *cmd= cli->findCommand(words[1]);
                if(!cmd)
                {
                    cliFailure(_("%s: no such command.\n"), words[1].c_str());
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
        string getName()            { return "stats"; }
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

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
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
        string getName()            { return "add-arcs"; }
        string getSynopsis()        { return getName() + " {:|<}"; }
        string getHelpText()        { return _("read a data set of arcs and add them to the graph. empty line terminates the set."); }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || !(hasDataSet||(inFile!=stdin)))
            {
                syntaxError();
                return CMD_FAILURE;
            }
            uint32_t oldSize= graph->size();
/*
            vector<uint32_t> record;
            for(unsigned lineno= 1; ; lineno++)
            {
                if(!Cli::readNodeIDRecord(inFile, record))
                {
                    cliError(_("couldn't read data set\n"));
                    return CMD_ERROR;
                }
                else if(record.size()==0)
                {
                    graph->resort(oldSize);
                    cliSuccess("\n");
                    return CMD_SUCCESS;
                }
                else if(record.size()!=2)
                {
                    cliError(_("invalid data record in line %u\n"), lineno);
                    return CMD_ERROR;
                }
                else
                {
                    if(record[0]==0 || record[1]==0) { cliError(_("invalid data record\n")); return CMD_ERROR; }
                    graph->addArc( record[0], record[1], false );
                    record.clear();
                }
            }
*/
            vector< vector<uint32_t> > dataset;
            if(!readNodeset(inFile, dataset, 2))
                return CMD_FAILURE;

            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                graph->addArc((*i)[0], (*i)[1], false);
            graph->resort(oldSize);

            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccRemoveArcs
// remove-arcs command
class ccRemoveArcs: public CliCommand_RTVoid
{
    public:
        string getName()            { return "remove-arcs"; }
        string getSynopsis()        { return getName() + " {:|<}"; }
        string getHelpText()        { return _("read a data set of arcs and remove them from the graph. empty line terminates the set."); }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=1 || !(hasDataSet||(inFile!=stdin)) )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            vector<uint32_t> record;
            while(true)
            {
                if(!Cli::readNodeIDRecord(inFile, record))
                {
                    cliError(_("couldn't read data set\n"));
                    return CMD_ERROR;
                }
                else if(record.size()==0)
                {
                    cliSuccess("\n");
                    return CMD_SUCCESS;
                }
                else if(record.size()!=2)
                {
                    cliError(_("invalid data record\n"));
                    return CMD_ERROR;
                }
                graph->eraseArc( record[0], record[1] );
                record.clear();
            }
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccReplaceNeighbors
// replace-predecessors/replace-successors commands
template<Digraph::NodeRelation searchType>
    class ccReplaceNeighbors: public CliCommand_RTVoid
{
    private:
        string name;

    public:
        ccReplaceNeighbors(const char *_name): name(_name)
        { }

        string getName()            { return name; }
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

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=2 || !(hasDataSet||(inFile!=stdin)) ||
                !Cli::isValidNodeID(words[1]) )
            {
                syntaxError();
                return CMD_FAILURE;
            }
            vector<uint32_t> record, newNeighbors;
            while(true)
            {
                if(!Cli::readNodeIDRecord(inFile, record))
                {
                    cliError(_("couldn't read data set\n"));
                    return CMD_ERROR;
                }
                if(record.size()==0) break;
                if(record.size()!=1)
                {
                    cliError(_("invalid data record\n"));
                    return CMD_ERROR;
                }
                newNeighbors.push_back(record[0]);
                record.clear();
            }
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
        string getName()            { return "clear"; }
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return _("clear the graph model."); }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
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
        string getName()            { return "shutdown"; }
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return _("shutdown the graph processor."); }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || hasDataSet|| (inFile!=stdin))
            {
                syntaxError();
                return CMD_FAILURE;
            }
            cliSuccess(_("shutting down pid %d.\n"), getpid());
            cli->quit();
            return CMD_SUCCESS;
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccFindPath
// find shortest path between nodes (findRoot==false) or path to nearest root node (findRoot==true)
template<bool findRoot> class ccFindPath: public CliCommand_RTArcList
{
    private:
        string name;

    public:
        ccFindPath(const char *_name): name(_name)
        { }

        string getName()            { return name; }
        string getSynopsis()        { return getName() + (findRoot? " X": " X Y"); }
        string getHelpText()
        {
            if(findRoot)
                return _("find the path from X to nearest root node. return data set of arcs representing the path.");
            else
                return _("find the shortest path from node X to node Y. return data set of arcs representing the path.");
        }

        CommandStatus execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile, vector<Digraph::arc> &result)
        {
            if( hasDataSet || !Cli::isValidNodeID(words[1]) ||
                (findRoot? (words.size()!=2):
                           (words.size()!=3 || !Cli::isValidNodeID(words[2]))) )
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
            lastStatusMessage= "NONE.\n";
            return CMD_NONE;
        }
};






Cli::Cli(Digraph *g): myGraph(g), doQuit(false)
{
    commands.push_back(new ccHelp(this));
    commands.push_back(new ccAddArcs());
    commands.push_back(new ccRemoveArcs());
    commands.push_back(new ccReplaceNeighbors<Digraph::PREDECESSORS>("replace-predecessors"));
    commands.push_back(new ccReplaceNeighbors<Digraph::DESCENDANTS>("replace-successors"));
    commands.push_back(new ccListNeighbors<Digraph::PREDECESSORS, true>("list-predecessors"));
    commands.push_back(new ccListNeighbors<Digraph::DESCENDANTS, true>("list-successors"));
    commands.push_back(new ccListNeighbors<Digraph::NEIGHBORS, true>("list-neighbors"));
    commands.push_back(new ccListNeighbors<Digraph::PREDECESSORS, false>("list-predecessors-nonrecursive"));
    commands.push_back(new ccListNeighbors<Digraph::DESCENDANTS, false>("list-successors-nonrecursive"));
//    commands.push_back(new ccListNeighbors<Digraph::NEIGHBORS, false>("list-neighbors-nonrecursive"));
    commands.push_back(new ccFindPath<false>("find-path"));
    commands.push_back(new ccFindPath<true>("find-root"));
    commands.push_back(new ccListNeighborless<false>("list-roots"));
    commands.push_back(new ccListNeighborless<true>("list-leaves"));
    commands.push_back(new ccStats());

    commands.push_back(new ccClear());
    commands.push_back(new ccShutdown());
}



int main()
{
    setlocale(LC_ALL, "");
    bindtextdomain("graphcore", "./messages");
    textdomain("graphcore");

    Digraph graph;
    Cli cli(&graph);

    cli.run();

    return 0;
}
