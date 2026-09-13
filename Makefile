CC = gcc
CFLAGS = -Wall -Wextra -pthread
LDLIBS = -lrt

all: server client

server: server.c common.h
	$(CC) $(CFLAGS) -o server server.c $(LDLIBS)

client: client.c common.h
	$(CC) $(CFLAGS) -o client client.c $(LDLIBS)

clean:
	rm -f server client