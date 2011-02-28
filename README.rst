

commands:


	- help_
	- add-arcs_
	- erase-arcs_
	- replace-predecessors_
	- replace-successors_
	- list-predecessors_
	- list-successors_
	- list-neighbors_
	- list-predecessors-nonrecursive_
	- list-successors-nonrecursive_
	- list-neighbors-nonrecursive_
	- find-path_
	- find-root_
	- list-roots_
	- list-leaves_
	- stats_
	- clear_
	- shutdown_

.. _help:

help


::


	syntax: help [COMMAND]
	get help on COMMAND/list commands.

.. _add-arcs:

add-arcs


::


	syntax: add-arcs {:|<}
	read a data set of arcs and add them to the graph. empty line terminates the set.

.. _erase-arcs:

erase-arcs


::


	syntax: erase-arcs {:|<}
	read a data set of arcs and erase them from the graph. empty line terminates the set.

.. _replace-predecessors:

replace-predecessors


::


	syntax: replace-predecessors NODE {:|<}
	read data set of nodes and replace predecessors of NODE with given set.

.. _replace-successors:

replace-successors


::


	syntax: replace-successors NODE {:|<}
	read data set of nodes and replace successors of NODE with given set.

.. _list-predecessors:

list-predecessors


::


	syntax: list-predecessors NODE DEPTH
	list NODE and its predecessors recursively up to DEPTH.

.. _list-successors:

list-successors


::


	syntax: list-successors NODE DEPTH
	list NODE and its successors recursively up to DEPTH.

.. _list-neighbors:

list-neighbors


::


	syntax: list-neighbors NODE DEPTH
	list NODE and its neighbors recursively up to DEPTH.

.. _list-predecessors-nonrecursive:

list-predecessors-nonrecursive


::


	syntax: list-predecessors-nonrecursive NODE
	list direct predecessors of NODE.

.. _list-successors-nonrecursive:

list-successors-nonrecursive


::


	syntax: list-successors-nonrecursive NODE
	list direct successors of NODE.

.. _list-neighbors-nonrecursive:

list-neighbors-nonrecursive


::


	syntax: list-neighbors-nonrecursive NODE
	list direct neighbors of NODE.

.. _find-path:

find-path


::


	syntax: find-path X Y
	find the shortest path from node X to node Y. return data set of arcs representing the path.

.. _find-root:

find-root


::


	syntax: find-root X
	find the path from X to nearest root node. return data set of arcs representing the path.

.. _list-roots:

list-roots


::


	syntax: list-roots
	list root nodes (nodes without predecessors).

.. _list-leaves:

list-leaves


::


	syntax: list-leaves
	list leaf nodes (nodes without descendants).

.. _stats:

stats


::


	syntax: stats
	print some statistics about the graph in the form of a name,value data set.
	names and their meanings:
	ArcCount	number of arcs
	ArcRamKiB	total RAM consumed by arc data, in KiB
	AvgPredecessors	average predecessors per node
	AvgSuccessors	average successors per node
	DataInvalid	nonzero if any obvious errors were found in graph data
	MaxNodeID	greatest node ID
	MinNodeID	lowest node ID
	NumDups	number of duplicates found (must be zero)

.. _clear:

clear


::


	syntax: clear
	clear the graph model.

.. _shutdown:

shutdown


::


	syntax: shutdown
	shutdown the graph processor.

