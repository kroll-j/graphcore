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

#ifndef MAIN_H
#define MAIN_H

// print debugging and other info to stderr. enable with 'make STDERR_DEBUGGING=1'.
#ifdef STDERR_DEBUGGING
void dodprint(const char *fmt, ...);
#define dprint dodprint
#else
#define dprint(x...)
#endif

#define U32MAX (0xFFFFFFFF)

// calculate average successors/predecessors per node in the stats command. 
// this takes a little long, so it's disabled.
// #define STATS_AVGNEIGHBORS 

// in add-arcs, use mark+remove to remove duplicates from data set instead of erase(). fast.
#define DUPCHECK_MARKRM

// in add-arcs, move duplicates upwards, then erase. slow, don't use.
//#define DUPCHECK_MOVERM

// in remove-arcs, use mark+remove instead of container's erase() method.
//#define REMOVEARCS_MARKRM

// use inplace_erase
#define REMOVEARCS_INPLACE

// in replace-*, use mark+remove instead of container's erase() method.
#define REPLACENEIGHBORS_MARKRM

typedef unordered_map<string, string> MetaMap;

double getTime();

#define DebugBreak() ({ __asm__ volatile ("int3"); })

#endif //MAIN_H
