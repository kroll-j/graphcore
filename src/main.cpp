// Graph Processor test code. (c) 2011 Johannes Kroll
// For testing purposes only. Not to be used in production environment.
#include <string.h>
#include <stdarg.h>
#include <iostream>
#include <cstdio>
#include <vector>
#include <deque>
#include <queue>
#include <set>
#include <map>
#include <algorithm>
#include <stdint.h>
#include <sys/time.h>
#include <pthread.h>
#include <readline/readline.h>
#include <readline/history.h>

using namespace std;

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
    return isatty(fileno(stdout));
}


class Digraph
{
    public:
        struct arc
        {
            uint32_t tail, head;
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
            addArc( (arc) { tail, head }, doSort );
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

        enum BFSType
        {
            NEIGHBORS= 0, PREDECESSORS, DESCENDANTS
        };
        // breitensuche / breadth-first-search
        void doBFS(vector<uint32_t> &resultNodes, map<uint32_t,uint32_t> &niveau,
                   uint32_t startNode, uint32_t depth, BFSType searchType= NEIGHBORS)
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
                if(searchType==PREDECESSORS) it.startPredecessors(next);
                else if(searchType==DESCENDANTS) it.startDescendants(next);
                else it.startNeighbors(next);
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

        // erase an arc from the graph
        void eraseArc(uint32_t tail, uint32_t head)
        {
            arcContainer::iterator it;
            arc value= (arc) { tail, head };
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
                    {
//                        printf("arc %d -> %d not found?!\n", it.getArc().tail, it.getArc().head);
                        return false;
                    }
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

        // print some statistics about the graph
        void printStats()
        {
            printf("%d arcs consuming %dMB of RAM\n", arcsByHead.size(), arcsByHead.size()*sizeof(arc)*2/(1024*1024));
            bool invalid= false;
            if(arcsByHead.size()!=arcsByTail.size())
                printf("array sizes don't match?!! (%d != %d)\n", arcsByHead.size(), arcsByTail.size()),
                invalid= true;
            int size= arcsByHead.size();
            int numDups= 0;
            uint32_t minNodeID= 0xFFFFFFFF, maxNodeID= 0;
            for(int i= 0; i<size; i++)
            {
                arc &h= arcsByHead[i];
                if(i<size-1 && h==arcsByHead[i+1])
                    numDups++, invalid= true;
                if(h.tail && h.tail<minNodeID) minNodeID= h.tail;
                if(h.head && h.head<minNodeID) minNodeID= h.head;
                if(h.tail>maxNodeID) maxNodeID= h.tail;
                if(h.head>maxNodeID) maxNodeID= h.head;
            }
            printf("lowest node ID: %u\ngreatest node ID: %u\n", minNodeID, maxNodeID);
            if(numDups) printf("%d duplicate arcs found.\n", numDups);
            if(invalid) puts("Graph data is invalid.");
            else puts("Graph data looks okay.");
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


    private:
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
            arc value= { 0, head };
            return lower_bound(arcsByHead.begin(), arcsByHead.end(), value, compByHead);
        }

        // find the position of first arc with given tail (lower bound)
        arcContainer::iterator findArcByTail(uint32_t tail)
        {
            arc value= { tail, 0 };
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



class Cli
{
    public:
        Cli(Digraph *g): myGraph(g), doQuit(false)
        {
        }

        ~Cli()
        {
        }

        // read and execute commands from stdin until eof or quit command
        void run()
        {
            char *command= 0;
            FILE *inRedir= 0, *outRedir= 0;
            bool commandHasDataSet;
            while(!doQuit)
            {
                if(inRedir) fclose(inRedir), inRedir= 0;
                if(outRedir) fclose(outRedir), outRedir= 0;
                if( (command= readline("> "))==0 ) return;
                char *completeCommand= strdup(command);
                char *d= strchr(command, '>');
                if(d)
                {
                    *d++= 0;
                    if(!(outRedir= fopen(getRedirFilename(d), "w")))
                    {
                        cmdFail("couldn't open output file");
                        continue;
                    }
                }
                d= strchr(command, '<');
                if(d)
                {
                    *d++= 0;
                    if(!(inRedir= fopen(getRedirFilename(d), "r")))
                    {
                        cmdFail("couldn't open input file");
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

    private:
        Digraph *myGraph;
        bool doQuit;

/*
        struct command
        {
            const char *name;
            void (Cli::*func)(vector<string> words);
            bool producesData;
            bool readsData;
        };

        static command commands[];
*/
        // execute a command
        void execute(char *command, bool hasDataSet, FILE *inRedir, FILE *outRedir)
        {
            vector<string> words= splitString(command);
            if(words.size()<1) return;

/*
            for(int i= 0; commands[i].name; i++)
            {
                if(words[0]==commands[i].name)
                {
                    if(commands[i].producesData)
                    {
                        if(outRedir)
                        else if(!hasDataSet) { cmdFail("syntax error"); continue; }
                    }
                    (this->*commands[i].func)(words);
                }
            }
*/

            FILE *outFile= (outRedir? outRedir: stdout);

            //c: command: list-neighbors NODE DEPTH
            //c: 	print data set of predecessors and successors of NODE recursively up to DEPTH.
            //c:
            //c: command: list-predecessors NODE DEPTH
            //c: 	print data set of predecessors of NODE recursively up to DEPTH.
            //c:
            //c: command: list-successors NODE DEPTH
            //c: 	print data set of successors of NODE recursively up to DEPTH.
            //c:
            //c: command: list-neighbors-nonrecursive NODE
            //c: 	print data set of predecessors and successors of NODE.
            //c:
            //c: command: list-predecessors-nonrecursive NODE
            //c: 	print data set of predecessors of NODE.
            //c:
            //c: command: list-successors-nonrecursive NODE
            //c: 	print data set of successors of NODE.
            //c:
            if(words[0]=="list-neighbors" || words[0]=="list-predecessors" || words[0]=="list-successors" ||
               words[0]=="list-neighbors-nonrecursive" || words[0]=="list-predecessors-nonrecursive" || words[0]=="list-successors-nonrecursive")
            {
                bool nonrecursive= words[0].find("nonrecursive")!=string::npos;
                if( (words.size()!=(nonrecursive? 2: 3)) || hasDataSet)
                {
                    cmdFail("syntax error");
                    return;
                }
                double d= getTime();
                vector<uint32_t> resultNodes;
                map<uint32_t,uint32_t> nodeNiveau;
                myGraph->doBFS(resultNodes, nodeNiveau, parseUint(words[1]), (words.size()==3? parseUint(words[2]): 1),
                               (words[0].find("neighbors")!=string::npos? Digraph::NEIGHBORS:
                                words[0].find("predecessors")!=string::npos? Digraph::PREDECESSORS:
                                Digraph::DESCENDANTS) );
                int begin= (words.size()==3? 0: 1);
                cmdOk("%d nodes, %fs%s", (int)resultNodes.size()-begin>=0? resultNodes.size()-begin: 0, getTime()-d, outRedir? "": ":");
                for(uint32_t i= begin; i<resultNodes.size(); i++)
//                    fprintf(outFile, "%u,%u\n", resultNodes[i], nodeNiveau[resultNodes[i]]);
					fprintf(outFile, "%u\n", resultNodes[i]);
                fprintf(outFile, "\n");
            }
            //c: command: add-arcs
            //c: 	read a data set of arcs and add them to the graph. empty line terminates the set.
            //c:
            else if(words[0]=="add-arcs")
            {
                if(words.size()!=1 || !(hasDataSet||inRedir) || outRedir)
                {
                    cmdFail("syntax error");
                    return;
                }
                FILE *f= (inRedir? inRedir: stdin);
                uint32_t oldSize= myGraph->size();
                vector<uint32_t> record;
                for(int lineno= 0; ; lineno++)
                {
                    if(!readUintRecord(f, record))
                    {
                        cmdErr("couldn't read data set");
                        return;
                    }
                    else if(record.size()==0)
                    {
                        myGraph->resort(oldSize);
                        cmdOk();
                        return;
                    }
                    else if(record.size()!=2)
                    {
                        cmdErr("invalid data record");
                        return;
                    }
                    else
                    {
                        if(record[0]==0 || record[1]==0) { cmdErr("invalid data record"); return; }
                        myGraph->addArc( record[0], record[1], false );
                    }
                    record.clear();
                }
            }
            //c: command: erase-arcs
            //c: 	read a data set of arcs and erase them from the graph. empty line terminates the set.
            //c:
            else if(words[0]=="erase-arcs")
            {
                if(words.size()!=1 || !(hasDataSet||inRedir) || outRedir)
                {
                    cmdFail("syntax error");
                    return;
                }
                FILE *f= (inRedir? inRedir: stdin);
                vector<uint32_t> record;
                while(true)
                {
                    if(!readUintRecord(f, record))
                    {
                        cmdErr("couldn't read data set");
                        return;
                    }
                    if(record.size()==0)
                    {
                        cmdOk();
                        return;
                    }
                    if(record.size()!=2)
                    {
                        cmdErr("invalid data record");
                        return;
                    }
                    myGraph->eraseArc( record[1], record[0] );
                    record.clear();
                }
            }
            //c: command: replace-predecessors NODE
            //c: 	read data set of nodes, replace predecessors of NODE with given set.
            //c:
            //c: command: replace-successors NODE
            //c: 	read data set of nodes, replace successors of NODE with given set.
            //c:
            else if( words[0]=="replace-predecessors" || words[0]=="replace-successors" )
            {
                if(words.size()!=2 || !(hasDataSet||inRedir) || outRedir)
                {
                    cmdFail("syntax error");
                    return;
                }
                FILE *f= (inRedir? inRedir: stdin);
                vector<uint32_t> record, newNeighbors;
                while(true)
                {
                    if(!readUintRecord(f, record))
                    {
                        cmdErr("couldn't read data set");
                        return;
                    }
                    if(record.size()==0) break;
                    if(record.size()!=1)
                    {
                        cmdErr("invalid data record");
                        return;
                    }
                    newNeighbors.push_back(record[0]);
                    record.clear();
                }
                if(myGraph->replaceNeighbors(parseUint(words[1]), newNeighbors, words[0]=="replace-successors"))
                    cmdOk();
                else
                    cmdErr("replaceNeighbors failed.");
            }
            //c: command: clear
            //c: 	clear the graph model.
            //c:
            else if(words[0]=="clear")
            {
                myGraph->clear();
                cmdOk();
            }
            //c: command: quit
            //c: command: q
            //c: 	quit program.
            //c:
            else if(words[0]=="quit" || words[0]=="q")
            {
                cmdOk();
                doQuit= true;
            }
            else if(words[0]=="list-by-tail")
            {
                myGraph->listArcsByTail(parseUint(words[1]), parseUint(words[2]));
                cmdOk();
            }
            else if(words[0]=="list-by-head")
            {
                myGraph->listArcsByHead(parseUint(words[1]), parseUint(words[2]));
                cmdOk();
            }
            else if(words[0]=="print-stats")
            {
                myGraph->printStats();
                cmdOk();
            }
            else
            {
                cmdFail("unknown command");
            }
        }

        // convert string to unsigned int
        static uint32_t parseUint(string str)
        {
            return strtoul(str.c_str(), 0, 0);
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

        // check if string forms a valid unsigned integer
        bool isValidUint(const string& s)
        {
            // allow only positive decimal digits
            for(int i= 0; s[i]; i++)
                if( !isdigit(s[i]) ) return false;
            return true;
        }

        // parse a data record in text form
        bool readUintRecord(FILE *f, vector<uint32_t> &ret)
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

        // print success/failure/error messages
        void cmdOk(const char *msg= "", ...)
        {
            printf("OK. ");
            va_list ap;
            va_start(ap, msg);
            vprintf(msg, ap);
            va_end(ap);
            puts("");
        }
        void cmdFail(const char *msg= "")
        {
            printf("FAILED! %s\n", msg);
        }
        void cmdErr(const char *msg= "")
        {
            printf("ERROR! %s\n", msg);
        }

        // split a string into words using given delimiters
        vector<string> splitString(char *str, const char *delim= " \n\t,")
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



int main()
{
    // gag readline when running non-interactively.
    if(!isInteractive()) rl_outstream= fopen("/dev/null", "w");

    Digraph graph;
    Cli cli(&graph);

    cli.run();

    return 0;
}
