CFLAGS = -O3 -Wall -ggdb 

DUCKINCLUDE=/home/ahu/git/duckdb/src/include
DUCKLIBS=/home/ahu/git/duckdb/build/release/src


CXXFLAGS:= -std=gnu++17 -Wall -O3 -ggdb -MMD -MP -fno-omit-frame-pointer -IIext/CLI11 \
	 -I${DUCKINCLUDE}
	 

# CXXFLAGS += -Wno-delete-non-virtual-dtor

PROGRAMS = ducktime
all: ${PROGRAMS}

-include *.d

clean:
	rm -f *~ *.o *.d 

ducktime: ducktime.o 
	$(CXX) -std=gnu++17 $^ -o $@ -pthread -Wl,-rpath=${DUCKLIBS} ${DUCKLIBS}/libduckdb.so  

