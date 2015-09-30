all: *.c *.h
	gcc -O3 -g -Wall -I/mnt/datastore/EUROSYS/CACHE_BENCH/libevent/bin/include *.c -L/mnt/datastore/EUROSYS/CACHE_BENCH/libevent/bin/lib -levent -lpthread -lm -D_GNU_SOURCE -o loader

clean:
	rm loader

