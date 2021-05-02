/* Copyright 2021 <> */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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
	load_balancer *bal = (load_balancer*)malloc(sizeof(*bal));
    bal->servers = (server_memory**)malloc(99999 * sizeof(*bal->servers));
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
    int pos = bin_search(main->ids, main->num_labels, key_hashed) % main->num_labels;
	*server_id = main->ids[pos].id % 100000;
    server_store(main->servers[*server_id], key, value);
}


char* loader_retrieve(load_balancer* main, char* key, int* server_id) {
	unsigned int key_hashed = hash_function_key(key);
	int pos = bin_search(main->ids, main->num_labels, key_hashed) % main->num_labels;
	*server_id = main->ids[pos].id % 100000;
	return server_retrieve(main->servers[*server_id], key);
}

int insert_sorted(struct server_info *v, int n, unsigned int x)
{
    int i = 0;
    unsigned int hashed_id = hash_function_servers(&x);
    while (i < n && v[i].hashed_id < hashed_id)
        i++;
    for (int j = n; j >= i; j--)
        v[j] = v[j - 1];
    v[i].id = x;
    v[i].hashed_id = hashed_id;
    return i;
}

int remove_nth(struct server_info *v, int n, int x)
{
    for (int i = x; i < n - 1; i++)
        v[i] = v[i + 1];
    return v[x % (n - 1)].id;
}

void loader_add_server(load_balancer* main, int server_id) {
    int pos;
    unsigned int hashed_id, temp_id;
    for (unsigned int k = 0; k < 3; k++)
    {
        temp_id = k*100000 + server_id;
        pos = insert_sorted(main->ids, main->num_labels, temp_id);
        hashed_id = hash_function_servers(&temp_id);
        main->num_labels += 1;
        hashtable_t *ht_to_check = main->servers[main->ids[(pos + 1) % main->num_labels].id % 100000]->ht;
        hashtable_t *ht_new = main->servers[main->ids[pos].id % 100000]->ht;
        for (unsigned int i = 0; i < ht_to_check->hmax; i++)
        {
            ll_node_t *q = ht_to_check->buckets[i]->head;
            while (q)
            {
                char *key = (char*)((struct info*)q->data)->key;
                char *value = (char*)((struct info*)q->data)->value;
                if (hash_function_key(key) < hashed_id)
                {
                    ht_put(ht_new, key, strlen(key) + 1, value, strlen(value) + 1);
                    ht_remove_entry(ht_to_check, key);
                }
                q = q->next;
            }
        }
    }
}


void loader_remove_server(load_balancer* main, int server_id) {
    int pos;
    unsigned int hashed_id, temp_id;
    for (unsigned int k = 0; k < 3; k++)
    {
        temp_id = k*100000 + server_id;
        hashed_id = hash_function_servers(&temp_id);
        pos = bin_search(main->ids, main->num_labels, hashed_id);
        pos = remove_nth(main->ids, main->num_labels, pos) % 100000;
        main->num_labels -= 1;
        hashtable_t *ht_to_check = main->servers[server_id % 100000]->ht;
        hashtable_t *ht_new = main->servers[pos]->ht;
        if (pos != server_id)
            for (unsigned int i = 0; i < ht_to_check->hmax; i++)
            {
                ll_node_t *q = ht_to_check->buckets[i]->head;
                while (q)
                {
                    char *key = (char*)((struct info*)q->data)->key;
                    char *value = (char*)((struct info*)q->data)->value;
                    ht_put(ht_new, key, strlen(key) + 1, value, strlen(value) + 1);
                    ht_remove_entry(ht_to_check, key);
                    q = q->next;
                }
            }
    }
}

void free_load_balancer(load_balancer* main) {
    for (int i = 0; i < 100000; i++)
        free_server_memory(main->servers[i]);
    free(main->servers);
    free(main);
}
