/* Copyright 2021 <> */
#include <stdlib.h>
#include <string.h>

#include "load_balancer.h"

struct server_info {
    unsigned int id, hashed_id;
};

struct load_balancer {
	struct server_info ids[400000];
    server_memory **servers;
    int num_labels;
};

unsigned int hash_function_servers(void *a) {
    unsigned int uint_a = *((unsigned int *)a);

    uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
    uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
    uint_a = (uint_a >> 16u) ^ uint_a;
    return uint_a;
}

unsigned int hash_function_key(void *a) {
    unsigned char *puchar_a = (unsigned char *) a;
    unsigned int hash = 5381;
    int c;

    while ((c = *puchar_a++))
        hash = ((hash << 5u) + hash) + c;

    return hash;
}


load_balancer* init_load_balancer() {
	load_balancer *bal = malloc(sizeof(*bal));
    bal->servers = malloc(99999 * sizeof(*bal->servers));
    for (int i = 0; i < 100000; i++)
        bal->servers[i] = init_server_memory();
    bal->num_labels = 0;
    return bal;
}

int bin_search(struct server_info *vect, int n, unsigned int x)
{
    int left = 0, right = n - 1, mid = (left + right) / 2;
    while (left <= right)
    {
        if (x == vect[mid].hashed_id)
            return mid;
        if (x > vect[mid].hashed_id)
        {
            left = mid + 1;
        }
        if (x < vect[mid].hashed_id)
        {
            right = mid - 1;
        }
        mid = (left + right) / 2;
    }
    return mid + 1;
}

void loader_store(load_balancer* main, char* key, char* value, int* server_id) {
    unsigned int key_hashed = hash_function_key(key);
	*server_id = bin_search(main->ids, main->num_labels, key_hashed) & main->num_labels;
    server_store(main->servers[*server_id], key, value);
}


char* loader_retrieve(load_balancer* main, char* key, int* server_id) {
	unsigned int key_hashed = hash_function_key(key);
	*server_id = bin_search(main->ids, main->num_labels, key_hashed) & main->num_labels;
	return server_retrieve(main->servers[*server_id], key);
}

void loader_add_server(load_balancer* main, int server_id) {
    if (main->num_labels == 0)
    {
        main->ids[0].id = server_id;
        main->ids[0].hashed_id = hash_function_servers(&server_id);
    }
    else
    {
        unsigned int i = 0, hashed_id = hash_function_servers(&server_id);
        while (i < main->num_labels && main->ids[i].hashed_id < hashed_id)
            i++;
        for (int j = main->num_labels; j >= i; j--)
            main->ids[j] = main->ids[j - 1];
        main->ids[i].id = server_id;
        main->ids[i].hashed_id = hashed_id;
    }
    main->num_labels += 1;
}


void loader_remove_server(load_balancer* main, int server_id) {
	/* TODO. */
}

void free_load_balancer(load_balancer* main) {
    /* TODO. */
}
