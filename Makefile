LDFLAGS=-lpthread -lreadline

all: 	graphtest graphtest.dbg

graphtest:	src/main.cpp
		g++ -O3 -fexpensive-optimizations src/main.cpp $(LDFLAGS) -ographtest

graphtest.dbg:	src/main.cpp
		g++ -ggdb src/main.cpp $(LDFLAGS) -ographtest.dbg
