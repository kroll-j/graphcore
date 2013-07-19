// corecommands.h: shared definitions for graphcore commands.
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

#ifndef CORECOMMANDS_H
#define CORECOMMANDS_H

#if !defined(CORECOMMANDS_BEGIN) || !defined(CORECOMMANDS_END) || !defined(CORECOMMAND)
#error "You're doing it wrong."
#endif

CORECOMMANDS_BEGIN
    CORECOMMAND("help",                 ACCESS_READ,    ccHelp(this));
    CORECOMMAND("add-arcs",             ACCESS_WRITE,   ccAddArcs());
    CORECOMMAND("remove-arcs",          ACCESS_WRITE,   ccRemoveArcs());
    CORECOMMAND("replace-predecessors", ACCESS_WRITE,   ccReplaceNeighbors<BDigraph::PREDECESSORS>());
    CORECOMMAND("replace-successors",   ACCESS_WRITE,   ccReplaceNeighbors<BDigraph::DESCENDANTS>());
    CORECOMMAND("traverse-predecessors",ACCESS_READ,    ccListNeighbors<BDigraph::PREDECESSORS, true>());
    CORECOMMAND("traverse-successors",  ACCESS_READ,    ccListNeighbors<BDigraph::DESCENDANTS, true>());
    CORECOMMAND("traverse-neighbors",   ACCESS_READ,    ccListNeighbors<BDigraph::NEIGHBORS, true>());
    CORECOMMAND("list-predecessors",    ACCESS_READ,    ccListNeighbors<BDigraph::PREDECESSORS, false>());
    CORECOMMAND("list-successors",      ACCESS_READ,    ccListNeighbors<BDigraph::DESCENDANTS, false>());
    CORECOMMAND("find-path",            ACCESS_READ,    ccFindPath<false>());
    CORECOMMAND("find-root",            ACCESS_READ,    ccFindPath<true>());
    CORECOMMAND("list-roots",           ACCESS_READ,    ccListNeighborless<false>());
    CORECOMMAND("list-leaves",          ACCESS_READ,    ccListNeighborless<true>());
    CORECOMMAND("stats",                ACCESS_READ,    ccStats());

#ifdef DEBUG_COMMANDS
    CORECOMMAND("list-by-tail",         ACCESS_READ,    ccListArcs<false>());
    CORECOMMAND("list-by-head",         ACCESS_READ,    ccListArcs<true>());
    CORECOMMAND("add-stuff",            ACCESS_WRITE,   ccAddStuff());
    CORECOMMAND("rm-stuff",             ACCESS_WRITE,   ccRMStuff());
    CORECOMMAND("malloc-stats",         ACCESS_READ,    ccMallocStats());
#endif

    CORECOMMAND("clear",                ACCESS_WRITE,   ccClear());
    CORECOMMAND("shutdown",             ACCESS_ADMIN,   ccShutdown());
    CORECOMMAND("quit",                 ACCESS_ADMIN,   ccShutdown());

    CORECOMMAND("protocol-version",     ACCESS_ADMIN,   ccProtocolVersion());

    CORECOMMAND("set-meta",             ACCESS_WRITE,   ccSetMeta());
    CORECOMMAND("get-meta",             ACCESS_READ,    ccGetMeta());
    CORECOMMAND("remove-meta",          ACCESS_WRITE,   ccRemoveMeta());
    CORECOMMAND("list-meta",            ACCESS_READ,    ccListMeta());
    
    CORECOMMAND("dump-graph",           ACCESS_ADMIN,   ccDumpGraph());
    CORECOMMAND("load-graph",           ACCESS_ADMIN,   ccLoadGraph());

    CORECOMMAND("find-cycles",          ACCESS_READ,    ccFindCycles<true>());

CORECOMMANDS_END


#endif // CORECOMMANDS_H
