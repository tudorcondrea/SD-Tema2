CC=gcc
CFLAGS=-std=c99 -Wall -Wextra -g
LOAD=load_balancer
SERVER=server
STRUCTURES=Hashtable LinkedList

.PHONY: build clean

build: tema2

tema2: main.o $(LOAD).o $(SERVER).o Hashtable.o LinkedList.o
	$(CC) -g $^ -o $@

main.o: main.c
	$(CC) $(CFLAGS) $^ -c

$(SERVER).o: $(SERVER).c $(SERVER).h
	$(CC) $(CFLAGS) $^ -c

$(LOAD).o: $(LOAD).c $(LOAD).h
	$(CC) $(CFLAGS) $^ -c

Hashtable.o: Hashtable.c Hashtable.h
	$(CC) $(CFLAGS) $^ -c

LinkedList.o: LinkedList.c LinkedList.h
	$(CC) $(CFLAGS) $^ -c

clean:
	rm -f *.o tema2 *.h.gch
