

commands:


	- help_
	- add-arcs_
	- remove-arcs_
	- replace-predecessors_
	- replace-successors_
	- traverse-predecessors_
	- traverse-successors_
	- traverse-neighbors_
	- list-predecessors_
	- list-successors_
	- find-path_
	- find-root_
	- list-roots_
	- list-leaves_
	- stats_
	- list-by-tail_
	- list-by-head_
	- clear_
	- shutdown_
	- quit_

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

.. _remove-arcs:

remove-arcs


::


	syntax: remove-arcs {:|<}
	read a data set of arcs and remove them from the graph. empty line terminates the set.

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

.. _traverse-predecessors:

traverse-predecessors


::


	syntax: traverse-predecessors NODE DEPTH
	list NODE and its predecessors recursively up to DEPTH.

.. _traverse-successors:

traverse-successors


::


	syntax: traverse-successors NODE DEPTH
	list NODE and its successors recursively up to DEPTH.

.. _traverse-neighbors:

traverse-neighbors


::


	syntax: traverse-neighbors NODE DEPTH
	list NODE and its neighbors recursively up to DEPTH.

.. _list-predecessors:

list-predecessors


::


	syntax: list-predecessors NODE
	list direct predecessors of NODE.

.. _list-successors:

list-successors


::


	syntax: list-successors NODE
	list direct successors of NODE.

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
	list leaf nodes (nodes without successors).

.. _stats:

stats


::


	syntax: stats
	print some statistics about the graph in the form of a name,value data set.
	names and their meanings:
	ArcCount	number of arcs
	ArcRamKiB	total RAM consumed by arc data, in KiB
	DataInvalid	nonzero if any obvious errors were found in graph data
	MaxNodeID	greatest node ID
	MinNodeID	lowest node ID
	NumDups	number of duplicates found (must be zero)

.. _list-by-tail:

list-by-tail


::


	syntax: list-by-tail INDEX [N]
	debugging: list N arcs starting from INDEX, sorted by tail

.. _list-by-head:

list-by-head


::


	syntax: list-by-head INDEX [N]
	debugging: list N arcs starting from INDEX, sorted by head

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

.. _quit:

quit


::


	syntax: quit
	shutdown the graph processor.

