all: *.c *.h
	gcc -O3  -Wall -levent  -pthread -lm -D_GNU_SOURCE  *.c -o loader

clean:
	rm loader
