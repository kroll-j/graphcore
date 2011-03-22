CCFLAGS=-Wall
LDFLAGS=-lpthread -lreadline

all: 		Release Debug

Release:	graphcore
Debug:		graphcore.dbg

graphcore:	src/main.cpp
		g++ $(CCFLAGS) -O3 -fexpensive-optimizations src/main.cpp $(LDFLAGS) -ographcore

graphcore.dbg:	src/main.cpp
		g++ $(CCFLAGS) -DDEBUG_COMMANDS -ggdb src/main.cpp $(LDFLAGS) -ographcore.dbg

# updatelang: update the language files
# running this will generate changes in the repository
updatelang:	#
		./update-lang.sh

test:		Release Debug
		python test/talkback.py test/graphcore.tb ./graphcore
