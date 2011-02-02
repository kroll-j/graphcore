// Graph Processor test code. (c) 2011 Johannes Kroll
// For testing purposes only. Not to be used in production environment. Not to be published.
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

using namespace std;


double getTime()
{
	timeval tv;
	gettimeofday(&tv, 0);
	return tv.tv_sec + tv.tv_usec*0.000001;
}


class Digraph
{
	public:
		struct arc
		{
			uint32_t tail, head;
//			bool operator< (arc a) const
//			{ return (a.tail==tail? a.head<head: a.tail<tail); }
		};

		Digraph()
		{
		}

		~Digraph()
		{
		}

		void addArc(arc a, bool doSort= true)
		{
			arcsByHead.push_back(a);
			arcsByTail.push_back(a);
			if(doSort)
				sort(arcsByTail.begin(), arcsByTail.end(), compByTail),
				sort(arcsByHead.begin(), arcsByHead.end(), compByHead);
		}

		void addArc(uint32_t tail, uint32_t head, bool doSort= true)
		{
			addArc( (arc) { tail, head }, doSort );
		}

/*
		void generateCompleteGraph(int nNodes)
		{
			arc *arcs= new arc[nNodes*nNodes];
			for(int i= 0; i<nNodes; i++)
			{
				for(int k= 0; k<nNodes; k++)
					arcs[i*nNodes+k].tail= i+1,
					arcs[i*nNodes+k].head= k+1;
			}
			addArcs(nNodes*nNodes, arcs);
		}
*/

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
//			printf("\rsorting by tail\n");
//			sort(arcsByTail.begin(), arcsByTail.end(), compByTail);
//			printf("sorting by head\n");
//			sort(arcsByHead.begin(), arcsByHead.end(), compByHead);
//			printf("\rdone.\n");
			printf("\rsorting...\n");
			double d= getTime();

			threadedSort(oldSize);
//			printf(" by tail\n");
//			stable_sort(arcsByTail.begin(), arcsByTail.end(), compByTail);
//			printf(" by head\n");
//			stable_sort(arcsByHead.begin(), arcsByHead.end(), compByHead);

//			printf(" by tail\n");
//			doMerge(arcsByTail.begin(), arcsByTail.begin()+oldSize, arcsByTail.end(), compByTail);
//			printf(" by head\n");
//			doMerge(arcsByHead.begin(), arcsByHead.begin()+oldSize, arcsByHead.end(), compByHead);

			printf("done in %fs\n", getTime()-d);
		}
/*
		void listDescendants(uint32_t node, int depth, int curDepth= 0)
		{
			arcContainer::iterator start= findArcByTail(node), it;
			for(it= start; it!=arcsByTail.end() && it->tail==node; it++)
				printf("%*s%d -> %d\n", curDepth, "", it->tail, it->head);

			if(curDepth<depth)
				for(it= start; it!=arcsByTail.end() && it->tail==node; it++)
				{
//					printf("%*s%d -> %d\n", curDepth, "", it->tail, it->head);
					if(it->head==it->tail) continue;
					if(circleSet.find(it->head)!=circleSet.end()) { printf("circle found\n"); continue; }
					circleSet.insert(it->head);
					listDescendants(it->head, depth, curDepth+1);
				}
		}

		void listPredecessors(uint32_t node, int depth, int curDepth= 0)
		{
			arcContainer::iterator start= findArcByHead(node), it;
			for(it= start; it!=arcsByHead.end() && it->head==node; it++)
				printf("%*s%d <- %d\n", curDepth, "", it->head, it->tail);

			if(curDepth<depth)
				for(it= start; it!=arcsByHead.end() && it->head==node; it++)
				{
//					printf("%*s%d <- %d\n", curDepth, "", it->head, it->tail);
					if(it->head==it->tail) continue;
					if(circleSet.find(it->tail)!=circleSet.end()) { printf("circle found\n"); continue; }
					circleSet.insert(it->tail);
					listPredecessors(it->tail, depth, curDepth+1);
				}
		}
*/
/*
		void printNeighbors(uint32_t node, int depth)
		{
			printf("node: %d\n", node);
			circleSet.clear();
			listPredecessors(node, depth);
			circleSet.clear();
			listDescendants(node, depth);
		}
*/

/*
		void printNeighbors(uint32_t node, int depth, int curDepth= 1)
		{
			if(curDepth==1)
			{
				printf("node: %d\n", node);
				circleSet.clear();
				circleSet.insert(node);
			}

			arcContainer::iterator it= findArcByTail(node);
			for(; it!=arcsByTail.end() && it->tail==node; it++)
			{
				if(circleSet.find(it->head)!=circleSet.end()) continue;
				circleSet.insert(it->head);
//				printf("%*s%d -> %d\n", curDepth, "", it->tail, it->head);
				printf("%d ", it->head);
				if(it->tail!=it->head && curDepth<depth)
					printNeighbors(it->head, depth, curDepth+1);
			}

			it= findArcByHead(node);
			for(; it!=arcsByHead.end() && it->head==node; it++)
			{
				if(circleSet.find(it->tail)!=circleSet.end()) continue;
				circleSet.insert(it->tail);
//				printf("%*s%d <- %d\n", curDepth, "", it->head, it->tail);
				printf("%d ", it->tail);
				if(it->tail!=it->head && curDepth<depth)
					printNeighbors(it->tail, depth, curDepth+1);
			}
		}
*/

		void checkDups()
		{
			for(uint32_t i= 0; i<size()-1; i++)
			{
				if(arcsByHead[i].tail==arcsByHead[i+1].tail && arcsByHead[i].head==arcsByHead[i+1].head)
					printf("dup: %d\n", i);
			}
		}

		void listArcsByHead()
		{
			for(uint32_t i= 0; i<arcsByHead.size(); i++)
				printf("%d -> %d\n", arcsByHead[i].tail, arcsByHead[i].head);
		}

		void listArcsByTail()
		{
			for(uint32_t i= 0; i<20 && i<arcsByTail.size(); i++)
				printf("%d -> %d\n", arcsByTail[i].tail, arcsByTail[i].head);
		}


		const vector<uint32_t>& getNeighbors(uint32_t node, int depth)
		{
			circleSet.clear();
			resultNodes.clear();
			printf("getNeighbors %d %d\n", node, depth);
			findNeighbors(node, depth);
			return resultNodes;
		}

		void printNeighbors(uint32_t node, int depth)
		{
			printf("finding neighbors...\n");
			double d= getTime();
			const vector<uint32_t>& result= getNeighbors(node, depth);
			d= getTime()-d;
			fprintf(stderr, "GET time: %f\n", d);

			printf("printing neighbors...\n");
			for(uint32_t i= 0; i<result.size(); i++)
				printf("%d ", result[i]);
			puts("");
		}

		void insertionTest()
		{

		}


		void clear()
		{
			arcsByTail.clear();
			arcsByHead.clear();
			circleSet.clear();
			resultNodes.clear();
		}

		void resort(uint32_t begin= 0)
		{
			threadedSort(begin);
		}

		uint32_t size()
		{
			return arcsByTail.size();
		}


		enum BFSType
		{
			NEIGHBORS= 0, PREDECESSORS, DESCENDANTS
		};
		void doBFS(vector<uint32_t> &resultNodes, map<uint32_t,uint32_t> &niveau,
				   uint32_t startNode, uint32_t depth, BFSType searchType= NEIGHBORS)
		{
			NeighbourIterator it(*this);
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
					uint32_t neighbour= *it;
//					printf("neighbour=%d\n", neighbour);
					if(niveau.find(neighbour)==niveau.end())
					{
						niveau.insert(make_pair(neighbour, curNiveau+1));
						resultNodes.push_back(neighbour);
						Q.push(neighbour);
					}
				}
			}
		}

		void eraseArc(uint32_t tail, uint32_t head)
		{
			arcContainer::iterator it;
			arc value= (arc) { tail, head };
			if( (it= lower_bound(arcsByHead.begin(), arcsByHead.end(), value, compByHead))!=arcsByHead.end() )
				arcsByHead.erase(it);
			if( (it= lower_bound(arcsByTail.begin(), arcsByTail.end(), value, compByTail))!=arcsByTail.end() )
				arcsByTail.erase(it);
		}




	private:
		typedef vector<arc> arcContainer;
		arcContainer arcsByTail, arcsByHead;
		set<uint32_t> circleSet;
		vector<uint32_t> resultNodes;

		// tiefensuche -- funktioniert so nicht mit tiefenbeschränkung!
		void findNeighbors(uint32_t node, int depth, int curDepth= 1)
		{
			arcContainer::iterator it;
			for(it= findArcByTail(node); it!=arcsByTail.end() && it->tail==node; it++)
			{
				if(circleSet.find(it->head)!=circleSet.end()) continue;
				circleSet.insert(it->head);
				resultNodes.push_back(it->head);
				if(it->tail!=it->head && curDepth<depth)
					findNeighbors(it->head, depth, curDepth+1);
			}

			for(it= findArcByHead(node); it!=arcsByHead.end() && it->head==node; it++)
			{
				if(circleSet.find(it->tail)!=circleSet.end()) continue;
				circleSet.insert(it->tail);
				resultNodes.push_back(it->tail);
				if(it->tail!=it->head && curDepth<depth)
					findNeighbors(it->tail, depth, curDepth+1);
			}
		}

		class NeighbourIterator
		{
			private:
				arcContainer::iterator it;
				bool byHead, switchToDescendants;
				Digraph &graph;
				uint32_t startNode;
				bool isFinished;
				bool checkFinished()
				{
					if( (byHead==false && (it==graph.arcsByTail.end() || it->tail!=startNode)) ||
						(byHead==true && (it==graph.arcsByHead.end() || it->head!=startNode)) )
						return true;
					else
						return false;
				}

			public:
				NeighbourIterator(Digraph &g): graph(g)
				{ }

				bool finished()
				{ return isFinished; }

				void startNeighbors(uint32_t startNode)
				{
					byHead= true;
					switchToDescendants= true;
					it= graph.findArcByHead(startNode);
					this->startNode= startNode;
					isFinished= checkFinished();
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
//							puts("switch");
							if( (it= graph.findArcByTail(startNode))==graph.arcsByTail.end() ||
								it->tail!=startNode ) isFinished= true;
							else byHead= false;
						}
					}
					else
					{
						if(++it==graph.arcsByTail.end() || it->tail!=startNode) isFinished= true;
					}
				}

				uint32_t operator*()
				{
//					printf(" (%u -> %u) \n", it->tail, it->head);
					if(isFinished) return 0;
					else return (byHead? it->tail: it->head);
				}
		};
		friend class NeighbourIterator;


		static bool compByTail(arc a, arc b)
		{ return (a.tail==b.tail? a.head<b.head: a.tail<b.tail); }

		static bool compByHead(arc a, arc b)
		{ return (a.head==b.head? a.tail<b.tail: a.head<b.head); }

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
		static void *sorterThread(void *a)
		{
			sorterThreadArg *arg= (sorterThreadArg*)a;
			doMerge(arg->begin, arg->mergeBegin, arg->end, arg->compFunc);
			return 0;
		}
		void threadedSort(int mergeBegin)
		{
			pthread_t threadID;
			sorterThreadArg arg= { arcsByHead.begin(), arcsByHead.begin()+mergeBegin, arcsByHead.end(), compByHead };
			pthread_create(&threadID, 0, sorterThread, &arg);
			doMerge(arcsByTail.begin(), arcsByTail.begin()+mergeBegin, arcsByTail.end(), compByTail);
			pthread_join(threadID, 0);
		}

		static void doMerge(arcContainer::iterator begin, arcContainer::iterator mergeBegin, arcContainer::iterator end,
						   bool (*compFunc)(arc a, arc b))
		{
			stable_sort(mergeBegin, end, compFunc);
			inplace_merge(begin, mergeBegin, end, compFunc);
		}
};


void readFileTest(Digraph &graph, const char *filename)
{
	FILE *f= fopen(filename, "r");
	char *line= NULL;
	size_t lineSize= 0;
	lineSize= getline(&line, &lineSize, f);
	printf("reading %s\n", filename);
	uint32_t numArcs= 0;
	uint32_t greatestT= 0, greatestH= 0, smallestT= 0xFFFFFFFF, smallestH= 0xFFFFFFFF;
	while(!feof(f))
	{
		if(getline(&line, &lineSize, f)==-1) break;
		char *child= line, *parent= line;
		while( *parent && *parent!='\t' && *parent!=',' ) parent++;
		while( *parent && (*parent=='\t' || *parent==',') ) parent++;
		Digraph::arc a= (Digraph::arc){ strtoul(parent, 0, 0), strtoul(child, 0, 0) };
		if(a.tail>greatestT) greatestT= a.tail;
		if(a.head>greatestH) greatestH= a.head;
		if(a.tail<smallestT) smallestT= a.tail;
		if(a.head<smallestH) smallestH= a.head;
		graph.addArc( a, false );
		numArcs++;
	}
	printf("%d arcs read\n", numArcs);
	printf("sorting\n");
	double d= getTime();
	graph.resort();
	d= getTime()-d;
	printf("%f s\n", d);

	printf("tails: %d/%d\nheads: %d/%d\n", smallestT, greatestT, smallestH, greatestH);

	for(int i= 0; i<100; i++)
	{
		double d= getTime();
//		graph.printNeighbors(10, 10);
		printf("NODE: %d\n", i);
//		graph.printNeighbors(i, 1);
		const vector<uint32_t> &result= graph.getNeighbors(i*97, 10);
		printf("%d neighbors found\n", result.size());
		d= getTime()-d;
		fprintf(stderr, "time: %f\n", d);
	}
}


class Cli
{
	public:
		Cli(Digraph *g): myGraph(g), doQuit(false)
		{
		}

		~Cli()
		{
		}

		void run()
		{
			char *command= 0;
			size_t nchars= 0;
			FILE *inRedir, *outRedir;
			bool commandHasDataSet;
			while(!doQuit)
			{
				if(inRedir) fclose(inRedir), inRedir= 0;
				if(outRedir) fclose(outRedir), outRedir= 0;
				printf("> ");
				if(getline(&command, &nchars, stdin)==-1)
				{
					if(feof(stdin)) return;
					cmdFail("i/o error"); continue;
				}
				char *d= strchr(command, '>');
				if(d)
				{
					*d++= 0;
					if(!(outRedir= fopen(getRedirFilename(d), "w")))
					{ cmdFail("couldn't open output file"); continue; }
				}
				d= strchr(command, '<');
				if(d)
				{
					*d++= 0;
					if(!(inRedir= fopen(getRedirFilename(d), "r")))
					{ cmdFail("couldn't open input file"); continue; }
				}
				d= strchr(command, ':');
				if(d)
					*d= 0, commandHasDataSet= true;
				else
					commandHasDataSet= false;
				execute(command, commandHasDataSet, inRedir, outRedir);
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

			//c: command: list-neighbors NODE [DEPTH]
			//c: 	print data set of predecessors/successors of NODE.
			//c:	if DEPTH is given, include NODE in the data set, and do recursion up to DEPTH.
			//c:
			//c: command: list-predecessors NODE DEPTH
			//c: 	print data set of predecessors of NODE.
			//c:	if DEPTH is given, include NODE in the data set, and do recursion up to DEPTH.
			//c:
			//c: command: list-successors NODE DEPTH
			//c: 	print data set of successors of NODE.
			//c:	if DEPTH is given, include NODE in the data set, and do recursion up to DEPTH.
			//c:
			if(words[0]=="list-neighbors" || words[0]=="list-predecessors" || words[0]=="list-successors")
			{
				if( (words.size()!=3&&words.size()!=2) || hasDataSet) { cmdFail("syntax error"); return; }
				double d= getTime();
				vector<uint32_t> resultNodes;
				map<uint32_t,uint32_t> nodeNiveau;
				myGraph->doBFS(resultNodes, nodeNiveau, parseUint(words[1]), (words.size()==3? parseUint(words[2]): 1),
							   (words[0].find("neighbors")!=string::npos? Digraph::NEIGHBORS:
								words[0].find("predecessors")!=string::npos? Digraph::PREDECESSORS:
								Digraph::DESCENDANTS) );
				int begin= (words.size()==3? 0: 1);
				cmdOk("%d nodes, %fs:", (int)resultNodes.size()-begin>=0? resultNodes.size()-begin: 0, getTime()-d);
				for(uint32_t i= begin; i<resultNodes.size(); i++)
					fprintf(outFile, "%u,%u\n", resultNodes[i], nodeNiveau[resultNodes[i]]);
//					fprintf(outFile, "%u\n", resultNodes[i]);
				fprintf(outFile, "\n");
			}
			//c: command: add-arcs
			//c: 	read a data set of arcs and add them to the graph. empty line terminates the set.
			//c:
			else if(words[0]=="add-arcs")
			{
				if(words.size()!=1 || !(hasDataSet||inRedir) || outRedir) { cmdFail("syntax error"); return; }
				FILE *f= (inRedir? inRedir: stdin);
				uint32_t oldSize= myGraph->size();
				vector<uint32_t> record;
				while(true)
				{
					if(!readRecord(f, record)) { cmdErr("couldn't read data set"); return; }
					if(record.size()==0) { myGraph->resort(oldSize); /*myGraph->checkDups();*/ cmdOk(); return; }
					if(record.size()!=2) { cmdErr("invalid data record"); return; }
					myGraph->addArc( record[1], record[0], false );
					record.clear();
				}
			}
			//c: command: erase-arcs
			//c: 	read a data set of arcs and erase them from the graph. empty line terminates the set.
			//c:
			else if(words[0]=="erase-arcs")
			{
				if(words.size()!=1 || !(hasDataSet||inRedir) || outRedir) { cmdFail("syntax error"); return; }
				FILE *f= (inRedir? inRedir: stdin);
				vector<uint32_t> record;
				while(true)
				{
					if(!readRecord(f, record)) { cmdErr("couldn't read data set"); return; }
					if(record.size()==0) { cmdOk(); return; }
					if(record.size()!=2) { cmdErr("invalid data record"); return; }
					myGraph->eraseArc( record[1], record[0] );
					record.clear();
				}
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
			//c: 	quit program.
			//c:
			else if(words[0]=="quit")
			{
				cmdOk();
				doQuit= true;
			}
			else if(words[0]=="test")
			{
				myGraph->listArcsByTail();
				cmdOk();
			}
			else
			{
				cmdFail("unknown command");
			}
		}

		static uint32_t parseUint(string str)
		{ return strtoul(str.c_str(), 0, 0); }

		static char *getRedirFilename(char *str)
		{
			while(isspace(*str)) str++;
			char *s= str;
			while(*s && !isspace(*s)) s++;
			*s= 0;
			return str;
		}

		bool readRecord(FILE *f, vector<uint32_t> &ret)
		{
			char *line= 0;
			uint32_t n= 0;
			if(getline(&line, &n, f)==-1)
			{
				if(!feof(f)) return false;
				else return true;
			}
			vector<string> strings= splitString(line);
			for(uint32_t i= 0; i<strings.size(); i++)
				ret.push_back(parseUint(strings[i]));
			free(line);
			return true;
		}

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
		{ printf("FAILED! %s\n", msg); }
		void cmdErr(const char *msg= "")
		{ printf("ERROR! %s\n", msg); }

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

//Cli::command Cli::commands[]=
//{
//	{ 0 }
//};


int main()
{
	Digraph graph;

//	readFileTest(graph, "/tmp/dewiki-cat-all.tsv");

	Cli cli(&graph);
	cli.run();

	return 0;

//	graph.generateRandomArcs(500, 10);
//	for(int i= 0; i<10; i++)
//	{
//		graph.printNeighbors(i, 500);
//		puts("----");
//	}
//	graph.printNeighbors(5, 1000);
//	graph.listArcsByTail();



/*
// 2.10
	graph.addArc( (Digraph::arc) { 1, 2 } );
	graph.addArc( (Digraph::arc) { 1, 5 } );
	graph.addArc( (Digraph::arc) { 2, 3 } );
	graph.addArc( (Digraph::arc) { 2, 4 } );
	graph.addArc( (Digraph::arc) { 3, 4 } );
	graph.addArc( (Digraph::arc) { 5, 4 } );
	graph.addArc( (Digraph::arc) { 5, 3 } );

	for(int i= 1; i<=5; i++)
		graph.printNeighbors(i, 1000),
		puts("\n------");

*/


//	srandom(time(0));

	for(int i= 10000; i>=10; i/= 10)
	{
		graph.clear();
		graph.generateRandomArcs(70000000, 200000);
		graph.generateRandomArcs(i, 200000);

		double d= getTime();
	//	graph.printNeighbors(10, 10);
		const vector<uint32_t> &result= graph.getNeighbors(10, 7);
		printf("%d neighbors found\n", result.size());
		d= getTime()-d;
		fprintf(stderr, "time: %f\n", d);
	}


//	graph.generateCompleteGraph(500);
//	graph.printNeighbors(2, 1);

    return 0;
}
