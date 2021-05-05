CC=gcc
CFLAGS=-std=c99 -Wall -Wextra -g

.PHONY: build clean

build: tema2

tema2: main.c Hashtable.c Hashtable.h LinkedList.c LinkedList.h load_balancer.c load_balancer.h server.c server.h
	$(CC) $(CFLAGS) $^ -o $@

clean:
	rm tema2
