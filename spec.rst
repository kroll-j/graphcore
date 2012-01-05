=======================
GraphCore Specification
=======================
GraphCore is a program for maintaining and processing large graphs in memory. It
provides a simple command oriented interface using the stdin/stdout streams. The
command interface is designed to be usable as a synchronized but sessionless
client/server communication protocol.

Data Model
------------
A GraphCore instance maintains a single, directed graph. The graph is represented
by a set of arcs, connecting two nodes (head and tail of the arc). Nodes that
are not part of arcs are unknown to GraphCore.

Nodes are represented by integer IDs in the range between 1 and 2^32.

Paths through the graph are represented as ordered lists of arcs.

Root nodes are nodes with no predecessors, leaf nodes are nodes with no successor.

Interface Principles
-----------------------
The interaction consists of commands, responses, and data-sets. Commands and
responses can be followed up by an inline data-set. This is indicated by the 
command resp. response ending with a colon (:). data-sets are terminated by 
a blank line.

When a client interacts with a GraphCore instance, it can always send a command 
(followed by a data-set, if appropriate), and then read the response for that
command (followed by a data-set, if present).


Commands
~~~~~~~~~
**Commands** consist of a single line, containing the command name and any
parameters. If the command ends with a colon (:), it is followed by a 
data set, terminated by an empty line. 

Note that if a command indicates that it is followed by a dataset, that
dataset must always be read completely from the input pipe, even if
the command is illegal or the dataset is invalid.

Commands are idempotent, executing the same command several times in a row
has the same effect as executing it only once. No state (except for the 
graph itself) is maintened between executing commands.

Commands that take a data-set as input may also load the data-set from a file.
To achieve this, the command is not terminated by a colon and followed by a 
data set, but it is instead followed in the same line by a smaller-than sign
(“<”) and the data file's name. This is analogous to the pipe-syntax used by
Unix shells. Trying to load data from a non-existing or malformed file
results in an error or failure  (see below). The data in the file must follow
the syntax for data-sets (see blow).

Note that if the command returns an empty result or a NONE-result (see
below), and the result is redirected to a file, an empty file shall be written.

Commands can also redirect their result into a file.  To achieve this, the command 
is followed by a larger-than sign (“>”) and target file's name, in the same line. 
This is analogous to the pipe-syntax used by Unix shells. Trying to redirect the 
output of a command that does not generate a result set results in a failure of
the command. Failure to actually write the data to the file results in error 
or failure (see below). The data written to the file follows the syntax for
data-sets (see blow).

Commands can be composited from multiple commands using **operators** to join them
(see below).

For a list of commands, see the Command Overview section below.

Responses
~~~~~~~~~~
**Responses** consist of a single line. If the line ends with a colon (:),
it is followed by a dataset, terminated by an empty line. 

All commands evict a single response. Responses indicate unambiguously if the
command was executed successfully. It has the following form::

  <`HEAD`> [`COMMENT`] [:]

The `HEAD` indicates the outcome of the command. can take the following values:

OK. [`COMMENT`] [:]
  The command was successful. If the line ends wit ha colon (:), it is followed
  by a data set. Note that if the output was redirected into a file, even 
  responses to commands that did return a data set are not followed by a colon.
  That is, the colon indicates an _inline_ data-set.

OK. [`COMMENT`]
  The command was successful, but does not return anything.

NONE. [`COMMENT`]
  The command was successful, but could not find a suitable result. This is different from
  returning the empty set.

FAILED! [`COMMENT`]
  The command was not executed, the graph did not change.

ERROR! [`COMMENT`]
  The command could not be completed successfully. The graph may have changed.

The `COMMENT` is an optional human readable string giving additional information. The final
column, if present, indicated that the response line is followed by a data set, which is in
turn terminated by a blank line.

Data-Sets
~~~~~~~~~~
**Data-sets** are represented as a CSV table, each data
record by a single line (terminated by CR+LF), data fields in
the line delimited by comma (“,”). This format represents a subset of RFC 4180. 
The data-set is terminated by a blank line. 

Note that a data-set may be empty. In this case, it only conists of a blank line.

A data-set representing a set of arcs  is presented as a data-set with two
columns containing node IDs, integers in the range between 1 and 2^32, in 
decimal notation. The two IDs in each line representing the end points of
an arc: the first column contains the head and the second column the tail of
the arc. 

A set of nodes would be represented accordingly, by a data-set with a single column,
containing the node IDs.

A path in the graph is represented as an ordered list of arcs, similar to the way
a set of arcs is represented.

Plain text lines can also be contained represented by a data-set. By convention, they
begin with an `#`. Any line starting with a # may be interpreted as consisting of a single
field. Any field separators can be treated as regular characters.

If a command encounters illegal lines or fields in a data-set, it will read all lines
of the data-set until the blank line, then returns an ERROR response (see above). If 
any line in the data-set is illegal, the command may or may not apply changes from any
of the valid lines.

Meta Variables
~~~~~~~~~~~~~~
Arbitrary variables can be queried and modified with the *-meta commands (see below).

Command Overview
-----------------
The following commands are supported:

help [`COMMAND`]:
  output help. If `COMMAND` is given, provide detailed help for the command. If 
  not, list the synopsis for all commands. The output is formatted as a data set
  in which each line starts with a "#" symbol to indicate that it contains plain text.

stats:
  returns runtime statistics, as a data-set with two columns. The first column contains
  the figure's name, the second column contains the figure's value. There may be any
  number of figures returned, however, one is required to be present: ``ArcCount``, 
  the number of arcs currently in the graph.

shutdown:
  terminates the GraphCore engine.

clear:
  clears the graph. After clear was run, there are no arcs in the graph.

add-arcs {:\|<}
  adds arcs to the graph. The arcs are provided as a two column data-set. 
  Duplicate arcs are ignored. 

remove-arcs {:\|<}
  removes arcs from the graph. The arcs are provided as a two column data-set.
  Removing arcs that are not currently in the graph has no effect. 

replace-predecessors `NODE` {:\|<}
  replace-predecessors removes all arcs that have `NODE` as their tail, and then adds
  arcs that have `NODE` as their tail and each node from the predecessor set as their head.
  The predecessor set is provided as a single column data-set.

replace-successors `NODE` {:\|<}
  replace-successors removes all arcs that have `NODE` as their head, and then adds
  arcs that have `NODE` as their head and each node from the successor set as their tail.
  The successor set is provided as a single column data-set.

traverse-predecessors `NODE` `DEPTH`
  returns a data-set containing all nodes from which `NODE` can be reached using a path
  of at most `DEPTH` arcs, including `NODE` itself. If `NODE` is not in the graph, the
  response is NONE. If node has no predecessors, but successors, or if `DEPTH` is 0,
  `NODE` itself is returned.

traverse-successors `NODE` `DEPTH`
  returns a data-set containing all nodes reachable from `NODE` using a path
  of at most `DEPTH` arcs, including `NODE` itself. If `NODE` is not in the graph, the
  response is NONE. If node has no successors, but predecessors, or if `DEPTH` is 0,
  `NODE` itself is returned.

traverse-neighbors `NODE` `DEPTH`
  analog to traverse-successors and traverse-predecessors, return a data set consisting of
  all neighbors (that is, predecessors and successors) of `NODE`, up to the given `DEPTH`.
  (note that this is different than the union of traverse-predecessors and traverse-successors
  with the same depth.)
  
list-predecessors `NODE`
  returns a data-set containing all nodes for which there is an arc that has `NODE`
  as the tail and that node as the head. This set does not contain `NODE` itself,
  unless an arc from `NODE` back to `NODE` (a loop) exists.
  If `NODE` is not in the graph, the response is NONE.
  If node has no predecessors, but successors, the empty set is returned.

list-successors `NODE`
  returns a data-set containing all nodes for which there is an arc that has `NODE`
  as the head and that node as the tail. This set does not contain `NODE` itself,
  unless an arc from `NODE` back to `NODE` (a loop) exists.
  If `NODE` is not in the graph, the response is NONE.
  If node has no successors, but predecessors, the empty set is returned.

find-path `X` `Y`
  returns a shortest path from node `X` to node `Y`, if such a path exists. If no
  such path exists, the response is NONE. If `X` and `Y` are the same node, the
  empty set is returned. Otherwise, the path is given as an ordered data-set
  of the arcs that constitute the path.

find-root `NODE`
  returns a shortest path to a node that has no predecessors, i.e. a root node. 
  If no such path exists, the response is NONE - this is the case if the node is
  not part of the graph, or it's part of a circle and no path to a root exists.
  If `NODE` itself is a root node, the empty set is returned.

list-roots
  list all nodes that have no predecessors, as a single column data-set. If there
  are no nodes in the graph, or all nodes are part of circles, the empty set is returned.

list-leaves
  list all nodes that have no successors, as a single column data-set. If there
  are no nodes in the graph, or all nodes are part of circles, the empty set is returned.

set-meta VARIABLE VALUE
  sets free-form meta VARIABLE to VALUE. 
  if the variable does not exist, it is created.
  variable names may contain alphabetic characters (a-z A-Z), digits (0-9), hyphens (-) and underscores (_),
  and must start with an alphabetic character, a hyphen or an underscore.

get-meta VARIABLE
  reads a meta variable. value is returned in the status line in the form 'VALUE: string'.
  if the variable does not exist, the command fails.

remove-meta VARIABLE
  removes the named meta variable. 
  if the variable does not exist, the command fails.

list-meta
  returns a data set of all existing meta variables in the form 'name,value'.
  the variables are listed in lexicographical order.
  if no variables are defined, the command returns the empty set.


Operator Overview
-------------------

Operators can be used to combine the output of two commands into one data-set. 
They are used with infox syntax::

  <COMMAND> <OPERATOR> <COMMAND>

This way, a composite command is formed. Note that if either operant fails, the composite
command also fails.

The following operator are currently specified:

intersection (&&):
  The intersection operator takes two operants, both of wich must return a set of nodes.
  The result of the composite command is a set of nodes that contains only the nodes
  that are in both, the result of the left operand, and the result of the right.
  If and only if either operant returns NONE, the result is NONE. 

subtraction (&&!):
  The subtraction operator takes two operants, both of wich must return a set of nodes.
  The result of the composite command is a set of nodes that contains only the nodes
  that are in the result of the left operand but not inthe result of the right operant.
  If and only if the left operant returns NONE, the result is NONE. If the right operant
  returns NONE, the result is the result of the left operant.

| 
| 
| `GraphServ, GraphCore (C) 2011, 2012 Wikimedia Deutschland, written by Johannes Kroll <jkroll at lavabit com>.`
| `Last update to this text: 2012/01/05`

