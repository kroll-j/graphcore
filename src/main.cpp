// Graph Processor core.
// (c) Wikimedia Deutschland, written by Johannes Kroll in 2011
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
    return isatty(STDOUT_FILENO);
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


// convenience macros for printing success/failure/error messages from the cli.
// if the protocol for those messages should ever have to be modified, change these.
#define SUCCESS_STR "OK."
#define FAIL_STR "FAILED!"
#define ERROR_STR "ERROR!"
#define cliSuccess(x...) printf(SUCCESS_STR " " x)
#define cliFailure(x...) printf(FAIL_STR " " x)
#define cliError(x...) printf(ERROR_STR " " x)

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
        virtual ReturnType getReturnType()= 0;
        void syntaxError()                  { cout << FAIL_STR " Syntax: " << getSynopsis() << endl; }
};


// base classes for cli commands. derive commands from these.
// execute() shall print the appropriate success/failure/error message
// and return true on success, false otherwise.

// cli commands which do not return a data set.
class CliCommand_RTVoid: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_NONE; }
        virtual bool execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)= 0;
};

// cli commands which return a node list data set.
class CliCommand_RTNodeList: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_NODE_LIST; }
        virtual bool execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                             vector<uint32_t> &result)= 0;
};

// cli commands which return some other data set. execute() must print the result to outFile.
class CliCommand_RTOther: public CliCommand
{
    public:
        ReturnType getReturnType() { return RT_OTHER; }
        virtual bool execute(vector<string> words, class Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)= 0;
};



class Cli
{
    public:
        Cli(Digraph *g);

        ~Cli()
        {
            for(unsigned int i= 0; i<commands.size(); i++)
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
                        cliFailure("couldn't open output file\n");
                        continue;
                    }
                }
                d= strchr(command, '<');
                if(d)
                {
                    *d++= 0;
                    if(!(inRedir= fopen(getRedirFilename(d), "r")))
                    {
                        cliFailure("couldn't open input file\n");
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

        // parse a data record in text form
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


    private:
        Digraph *myGraph;
        bool doQuit;

        vector<CliCommand*> commands;

        // execute a command
        void execute(char *command, bool hasDataSet, FILE *inRedir, FILE *outRedir)
        {
            vector<string> words= splitString(command);
            if(words.size()<1) return;

            FILE *outFile= (outRedir? outRedir: stdout);
            FILE *inFile= (inRedir? inRedir: stdin);
            CliCommand *cmd= findCommand(words[0]);

            if(cmd)
            {
                switch(cmd->getReturnType())
                {
                    case CliCommand::RT_NODE_LIST:
                    {
                        vector<uint32_t> result;
                        if( ((CliCommand_RTNodeList*)cmd)->execute(words, this, myGraph, hasDataSet, inFile, outFile, result) )
                        {
                            for(size_t i= 0; i<result.size(); i++)
                                fprintf(outFile, "%u\n", result[i]);
                            fprintf(outFile, "\n");
                        }
                        break;
                    }
                    case CliCommand::RT_ARC_LIST:
                    {
                        // todo.
                        break;
                    }
                    case CliCommand::RT_OTHER:
                        ((CliCommand_RTOther*)(cmd))->execute(words, this, myGraph, hasDataSet, inFile, outFile);
                        break;
                    case CliCommand::RT_NONE:
                        if(outFile!=stdout) cliFailure("output redirection not possible for this command.\n");
                        else ((CliCommand_RTVoid*)cmd)->execute(words, this, myGraph, hasDataSet, inFile);
                        break;
                }
            }
            else
                cliFailure("no such command.\n");
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

///////////////////////////////////////////////////////////////////////////////////////////
// ccListNeighbors
// template class for list-* commands
template<Digraph::BFSType searchType, bool recursive>
    class ccListNeighbors: public CliCommand_RTNodeList
{
    private:
        string name;

    public:
        ccListNeighbors(const char *_name): name(_name)
        { }

        string getName()            { return name; }
        string getSynopsis()        { return getName() + " NODE" + (recursive? " DEPTH": ""); }
        string getHelpText()
        {
            string what= (searchType==Digraph::NEIGHBORS? "neighbors":
                          searchType==Digraph::PREDECESSORS? "predecessors": "successors");
            return "list " + (recursive?
                              "NODE and its " + what + " recursively up to DEPTH." :
                              "direct " + what + " of NODE.");
        }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile,
                     vector<uint32_t> &result)
        {
            if( (words.size()!=(recursive? 3: 2)) || hasDataSet)
            {
                syntaxError();
                return false;
            }
            map<uint32_t,uint32_t> nodeNiveau;
            double d= getTime();
            graph->doBFS(result, nodeNiveau, Cli::parseUint(words[1]),
                         (recursive? Cli::parseUint(words[2]): 1),
                         searchType);
            if(!recursive && result.size()) result.erase(result.begin());
            cliSuccess("%u nodes, %fs%s\n", result.size(), getTime()-d, outFile==stdout? ":": "");
            return true;
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
        string getSynopsis()        { return getName() + " [COMMAND]"; }
        string getHelpText()        { return "get help on COMMAND/on all commands."; }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile, FILE *outFile)
        {
            if(words.size()>2 || hasDataSet)
            {
                syntaxError();
                return false;
            }
            if(words.size()==2)
            {
                CliCommand *cmd= cli->findCommand(words[1]);
                if(!cmd)
                {
                    cliFailure("No such command.\n");
                    return false;
                }
                cliSuccess(":\n");
                cout << "# " << cmd->getSynopsis() << endl << "# " << cmd->getHelpText() << endl;
            }
            else
            {
                cliSuccess("available commands:\n");
                vector<CliCommand*> &commands= cli->getCommands();
                for(unsigned i= 0; i<commands.size(); i++)
                    cout << "# " << commands[i]->getSynopsis() << endl;
            }
            cout << endl;
            return true;
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
        string getHelpText()        { return "read a data set of arcs and add them to the graph. empty line terminates the set."; }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || !(hasDataSet||(inFile!=stdin)))
            {
                syntaxError();
                return false;
            }
            uint32_t oldSize= graph->size();
            vector<uint32_t> record;
            for(int lineno= 0; ; lineno++)
            {
                if(!Cli::readUintRecord(inFile, record))
                {
                    cliError("couldn't read data set\n");
                    return false;
                }
                else if(record.size()==0)
                {
                    graph->resort(oldSize);
                    cliSuccess("\n");
                    return true;
                }
                else if(record.size()!=2)
                {
                    cliError("invalid data record\n");
                    return true;
                }
                else
                {
                    if(record[0]==0 || record[1]==0) { cliError("invalid data record\n"); return false; }
                    graph->addArc( record[0], record[1], false );
                }
                record.clear();
            }
        }
};


/*
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
*/


///////////////////////////////////////////////////////////////////////////////////////////
// ccEraseArcs
// erase-arcs command
class ccEraseArcs: public CliCommand_RTVoid
{
    public:
        string getName()            { return "erase-arcs"; }
        string getSynopsis()        { return getName() + " {:|<}"; }
        string getHelpText()        { return "read a data set of arcs and erase them from the graph. empty line terminates the set."; }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if( words.size()!=1 || !(hasDataSet||(inFile!=stdin)) )
            {
                syntaxError();
                return false;
            }
            vector<uint32_t> record;
            while(true)
            {
                if(!Cli::readUintRecord(inFile, record))
                {
                    cliError("couldn't read data set\n");
                    return false;
                }
                else if(record.size()!=2)
                {
                    cliError("invalid data record\n");
                    return false;
                }
                else if(record.size()==0)
                {
                    cliSuccess("\n");
                    return true;
                }
                graph->eraseArc( record[0], record[1] );
                record.clear();
            }
        }
};


/*
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
*/

///////////////////////////////////////////////////////////////////////////////////////////
// ccReplaceNeighbors
// replace-predecessors/replace-successors commands
template<Digraph::BFSType searchType>
    class ccReplaceNeighbors: public CliCommand_RTVoid
{
    private:
        string name;

    public:
        ccReplaceNeighbors(const char *_name): name(_name)
        { }

        string getName()            { return name; }
        string getSynopsis()        { return getName() + " NODE {:|<}"; }
        string getHelpText()
        {
            return string("read data set of nodes and replace ") +
                   (searchType==Digraph::PREDECESSORS? "predecessors": "successors") +
                   " of NODE with given set.";
        }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=2 || !(hasDataSet||(inFile!=stdin)))
            {
                syntaxError();
                return;
            }
            vector<uint32_t> record, newNeighbors;
            while(true)
            {
                if(!Cli::readUintRecord(inFile, record))
                {
                    cliError("couldn't read data set\n");
                    return;
                }
                if(record.size()==0) break;
                if(record.size()!=1)
                {
                    cliError("invalid data record\n");
                    return;
                }
                newNeighbors.push_back(record[0]);
                record.clear();
            }
            if(graph->replaceNeighbors(Cli::parseUint(words[1]), newNeighbors, searchType==Digraph::DESCENDANTS))
                cliSuccess("\n");
            else
                cliError("internal error: Digraph::replaceNeighbors() failed.\n");
        }
};


/*
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
*/


///////////////////////////////////////////////////////////////////////////////////////////
// ccClear
// clear command.
class ccClear: public CliCommand_RTVoid
{
    public:
        string getName()            { return "clear"; }
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return "clear the graph model."; }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || hasDataSet || (inFile!=stdin))
            {
                syntaxError();
                return false;
            }
            graph->clear();
            cliSuccess("\n");
        }
};


///////////////////////////////////////////////////////////////////////////////////////////
// ccShutdown
// shutdown command.
class ccShutdown: public CliCommand_RTVoid
{
    private:
        string name;

    public:
        string getName()            { return name; }
        string getSynopsis()        { return getName(); }
        string getHelpText()        { return "shutdown the graph processor."; }

        bool execute(vector<string> words, Cli *cli, Digraph *graph, bool hasDataSet, FILE *inFile)
        {
            if(words.size()!=1 || hasDataSet|| (inFile!=stdin))
            {
                syntaxError();
                return false;
            }
            cliSuccess("\n");
        }
};




Cli::Cli(Digraph *g): myGraph(g), doQuit(false)
{
    commands.push_back(new ccHelp(this));
    commands.push_back(new ccAddArcs());
    commands.push_back(new ccEraseArcs());
    commands.push_back(new ccListNeighbors<Digraph::DESCENDANTS, true>("list-successors"));
    commands.push_back(new ccListNeighbors<Digraph::PREDECESSORS, true>("list-predecessors"));
    commands.push_back(new ccListNeighbors<Digraph::NEIGHBORS, true>("list-neighbors"));
    commands.push_back(new ccListNeighbors<Digraph::DESCENDANTS, false>("list-successors-nonrecursive"));
    commands.push_back(new ccListNeighbors<Digraph::PREDECESSORS, false>("list-predecessors-nonrecursive"));
    commands.push_back(new ccListNeighbors<Digraph::NEIGHBORS, false>("list-neighbors-nonrecursive"));
}



int main()
{
    // gag readline when running non-interactively.
    if(!isInteractive()) rl_outstream= fopen("/dev/null", "w");

    Digraph graph;
    Cli cli(&graph);

    cli.run();

    return 0;
}
