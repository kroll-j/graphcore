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
#include "main.h"
#include "digraph.h"


enum CommandStatus
{
    CORECMDSTATUSCODES
};


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

void dodprint(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vfprintf(stderr, fmt, ap);
	va_end(ap);
}



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
        MetaMap meta;
    
        CoreCli(BDigraph *g);

        // read and execute commands from stdin until eof or quit command
        void run()
        {
            char *command= 0;
            FILE *inRedir= 0, *outRedir= 0;
            bool commandHasDataSet;
            try
            {
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
            catch(exception& ex)
            {
                dprint("unhandled exception: %s\nterminating.", ex.what());
                exit(1);
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
                                            for(vector<uint32_t>::iterator it= mergeResult.begin(); it!=end; ++it)
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
            for(map<string, BDigraph::statInfo>::iterator i= info.begin(); i!=info.end(); ++i)
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
            for(map<string, BDigraph::statInfo>::iterator i= info.begin(); i!=info.end(); ++i)
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
                    if(record.empty())
                    {
                        if(!ok) return CMD_ERROR;
                        graph->resort(oldSize, oldSize);
                        cliSuccess("\n");
                        return CMD_SUCCESS;
                    }
                    else if(record.size()!=2)
                    {
                        if(ok) cliError(_("error reading data set: record size %zu, should be 2. (line %u)\n"), record.size(), lineno);
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
			
			volatile double tStart= getTime();

            vector< vector<uint32_t> > dataset;
            if(!readNodeset(inFile, dataset, 2))
                return CMD_FAILURE;
			
			if(!dataset.empty())
			{
#ifdef REMOVEARCS_MARKRM
				for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); ++i)
					graph->queueArcForRemoval((*i)[0], (*i)[1]);
				graph->removeQueuedArcs();
#else
				for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); ++i)
					graph->eraseArc((*i)[0], (*i)[1]);
#endif
			}

			dprint("remove-arcs: %zu arcs removed in %3.0fms\n", dataset.size(), 
				(getTime()-tStart)*1000);

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

			dprint("%s %s\n", getName().c_str(), words[1].c_str());
			
			volatile double tStart= getTime();
			
            vector<uint32_t> newNeighbors;
            vector< vector<uint32_t> > dataset;

            if(!readNodeset(inFile, dataset, 1))
                return CMD_FAILURE;
            newNeighbors.reserve(dataset.size());
            for(vector< vector<uint32_t> >::iterator i= dataset.begin(); i!=dataset.end(); i++)
                newNeighbors.push_back((*i)[0]);
			
			volatile double tRead= getTime();
            
            if(!Cli::isValidNodeID(words[1])) 
            {
                cliFailure(_("invalid node ID '%s'\n"), words[1].c_str());
                return CMD_FAILURE;
            }
            
            uint32_t node= Cli::parseUint(words[1]);

            /// xxxx maybe put this logic into Digraph::replaceNeighbors()
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
            map<uint32_t,BDigraph::BFSnode> nodeInfo;
            graph->doBFS2<BDigraph::findAll> (node, 0, 1,
                                             oldNeighbors, nodeInfo, searchType);
            if(oldNeighbors.size()) oldNeighbors.erase(oldNeighbors.begin());   // remove the node itself.
            stable_sort(oldNeighbors.begin(), oldNeighbors.end());
            // build diff:
            uint32_t diffbuf[ oldNeighbors.size()+newNeighbors.size() ];
            // [diffbuf+0, idx_added): newly added neighbors
            auto idx_added= set_difference(newNeighbors.begin(),newNeighbors.end(), oldNeighbors.begin(),oldNeighbors.end(), diffbuf+0);
            // [idx_added, idx_removed): removed neighbors
            auto idx_removed= set_difference(oldNeighbors.begin(),oldNeighbors.end(), newNeighbors.begin(),newNeighbors.end(), idx_added);
            dprint("%s: oldNeighbors: %zu, newNeighbors: %zu, added: %zu removed: %zu\n", 
                getName().c_str(), oldNeighbors.size(), newNeighbors.size(), idx_added-(&diffbuf[0]), idx_removed-idx_added);
            if(idx_added-diffbuf<20 && idx_removed-idx_added<20)
            {
                dprint("added: \n");
                for(int i= 0; i<idx_added-diffbuf; i++)
                    dprint("    %08d\n", diffbuf[i]);
                dprint("removed: \n");
                for(int i= idx_added-diffbuf; i<idx_removed-diffbuf; i++)
                    dprint("    %08d\n", diffbuf[i]);
            }
            /// xxxx
            
			size_t sizeBefore= graph->size();

            if(idx_removed-idx_added==0)
            {
                if(idx_added-diffbuf==0)
                {
                    // nothing to do.
                    dprint("* no diff.\n");
                    cliSuccess("\n");
                    return CMD_SUCCESS;
                }
                else
                {
                    // only neighbors added, none removed. addArc/mergesort is faster than replacing in this case.
                    if(searchType==BDigraph::PREDECESSORS)
                        // add predecessors
                        for(auto it= diffbuf+0; it!=idx_added; ++it)
                            graph->addArc(*it, node, false);
                    else
                        // add descendants
                        for(auto it= diffbuf+0; it!=idx_added; ++it)
                            graph->addArc(node, *it, false);
                    // merge in new neighbors
                    graph->resort(sizeBefore, sizeBefore);
                    volatile double tEnd= getTime(); 
                    dprint("%s times (add only): %zu added, read dataset %3ums, diff+add %3ums, overall %3ums\n", 
                           getName().c_str(), idx_added-diffbuf, 
                           unsigned((tRead-tStart)*1000), unsigned((tEnd-tRead)*1000), unsigned((tEnd-tStart)*1000));
                    cliSuccess("\n");
                    return CMD_SUCCESS;
                }
            }
			else
            {
                // default: overwrite and minimum resort
                if(graph->replaceNeighbors(node, newNeighbors, 
                                            searchType==BDigraph::DESCENDANTS))
                {
                    volatile double tEnd= getTime(); 
                    dprint("%s times: %zu new neighbors, size diff %+d, read dataset %3ums, diff+replaceNeighbors %3ums, overall %3ums\n", 
                           getName().c_str(), newNeighbors.size(), int(graph->size()-sizeBefore), 
                           unsigned((tRead-tStart)*1000), unsigned((tEnd-tRead)*1000), unsigned((tEnd-tStart)*1000));
                    cliSuccess("\n");
                    return CMD_SUCCESS;
                }
                else
                {
                    cliError(_("internal error: BDigraph::replaceNeighbors() failed.\n"));
                    return CMD_ERROR;
                }
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
                while(!rmQueue.empty())
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

            MetaMap::iterator it= cli->meta.find(words[1]);
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
            
            for(auto it= cli->meta.begin(); it!=cli->meta.end(); ++it)
                sortedVars.push_back(*it);
            
            std::sort(sortedVars.begin(), sortedVars.end());
            
            for(auto it= sortedVars.begin(); it!=sortedVars.end(); ++it)
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
			if(!graph->serialize(cli->meta, words[1].c_str(), error))
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
			if(!graph->deserialize(cli->meta, words[1].c_str(), error))
			{
				cliFailure( _("BDigraph::deserialize failed with reason: %s\n"), error.c_str() );
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
