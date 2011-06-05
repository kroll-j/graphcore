// corecommands.h: shared definitions for graphcore commands.
// (c) Wikimedia Deutschland, written by Johannes Kroll in 2011

#ifndef CORECOMMANDS_H
#define CORECOMMANDS_H

#if !defined(CORECOMMANDS_BEGIN) || !defined(CORECOMMANDS_END) || !defined(CORECOMMAND)
#error "You're doing it wrong."
#endif

CORECOMMANDS_BEGIN
    CORECOMMAND("help",                 ACCESS_READ,    ccHelp(this));
    CORECOMMAND("add-arcs",             ACCESS_WRITE,   ccAddArcs());
    CORECOMMAND("remove-arcs",          ACCESS_WRITE,   ccRemoveArcs());
    CORECOMMAND("replace-predecessors", ACCESS_WRITE,   ccReplaceNeighbors<Digraph::PREDECESSORS>());
    CORECOMMAND("replace-successors",   ACCESS_WRITE,   ccReplaceNeighbors<Digraph::DESCENDANTS>());
    CORECOMMAND("traverse-predecessors",ACCESS_READ,    ccListNeighbors<Digraph::PREDECESSORS, true>());
    CORECOMMAND("traverse-successors",  ACCESS_READ,    ccListNeighbors<Digraph::DESCENDANTS, true>());
    CORECOMMAND("traverse-neighbors",   ACCESS_READ,    ccListNeighbors<Digraph::NEIGHBORS, true>());
    CORECOMMAND("list-predecessors",    ACCESS_READ,    ccListNeighbors<Digraph::PREDECESSORS, false>());
    CORECOMMAND("list-successors",      ACCESS_READ,    ccListNeighbors<Digraph::DESCENDANTS, false>());
    CORECOMMAND("find-path",            ACCESS_READ,    ccFindPath<false>());
    CORECOMMAND("find-root",            ACCESS_READ,    ccFindPath<true>());
    CORECOMMAND("list-roots",           ACCESS_READ,    ccListNeighborless<false>());
    CORECOMMAND("list-leaves",          ACCESS_READ,    ccListNeighborless<true>());
    CORECOMMAND("stats",                ACCESS_READ,    ccStats());

#ifdef DEBUG_COMMANDS
    CORECOMMAND("list-by-tail",         ACCESS_READ,    ccListArcs<false>());
    CORECOMMAND("list-by-head",         ACCESS_READ,    ccListArcs<true>());
    CORECOMMAND("add-stuff",            ACCESS_WRITE,   ccAddStuff());
    CORECOMMAND("malloc-stats",         ACCESS_READ,    ccMallocStats());
#endif

    CORECOMMAND("clear",                ACCESS_WRITE,   ccClear());
    CORECOMMAND("shutdown",             ACCESS_ADMIN,   ccShutdown());
    CORECOMMAND("quit",                 ACCESS_ADMIN,   ccShutdown());

    CORECOMMAND("protocol-version",     ACCESS_ADMIN,   ccProtocolVersion());
CORECOMMANDS_END


#endif // CORECOMMANDS_H
