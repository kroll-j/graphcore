LDFLAGS=-lpthread -lreadline

all: 	graphcore graphcore.dbg

graphcore:	src/main.cpp
		g++ -O3 -fexpensive-optimizations src/main.cpp $(LDFLAGS) -ographcore

graphcore.dbg:	src/main.cpp
		g++ -ggdb src/main.cpp $(LDFLAGS) -ographcore.dbg
