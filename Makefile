LDFLAGS=-lpthread

all: 	graphtest graphtest.dbg

graphtest:	src/main.cpp
		g++ -O3 src/main.cpp $(LDFLAGS) -ographtest

graphtest.dbg:	src/main.cpp
		g++ -O3 src/main.cpp $(LDFLAGS) -ographtest.dbg