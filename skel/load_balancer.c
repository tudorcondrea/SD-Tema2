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
	load_balancer *bal = malloc(sizeof(*bal));
    bal->servers = calloc(100000, sizeof(server_memory*));
    bal->num_labels = 0;
    return bal;
}

int bin_search(struct server_info *vect, int n, unsigned int x)
{
    if (x < vect[0].hashed_id)
        return 0;
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
	*server_id = main->ids[pos].id;
    /*printf("\n%010u %d\n", key_hashed, *server_id);
    for (int i = 0; i < main->num_labels; i++)
        printf("%d ", main->ids[i].id);
    printf("\n");
    for (int i = 0; i < main->num_labels; i++)
        printf("%010u ", main->ids[i].hashed_id);
    printf("\n");*/
    *server_id %= 100000;
    server_store(main->servers[*server_id], key, value);
}


char* loader_retrieve(load_balancer* main, char* key, int* server_id) {
	unsigned int key_hashed = hash_function_key(key);
	int pos = bin_search(main->ids, main->num_labels, key_hashed) % main->num_labels;
	*server_id = main->ids[pos].id;
    //printf("\n%010u %d\n", key_hashed, *server_id);
    *server_id %= 100000;
    /*for (int i = 0; i < main->num_labels; i++)
        printf("%d ", main->ids[i].id);
    printf("\n");
    for (int i = 0; i < main->num_labels; i++)
        printf("%u ", main->ids[i].hashed_id);
    printf("\n");
    for (int i = 0; i < main->servers[*server_id]->ht->hmax; i++)
    {
        ll_node_t *q = main->servers[*server_id]->ht->buckets[i]->head;
        while (q != NULL)
        {
            printf("%010u %s\n", hash_function_key((char*)((struct info*)q->data)->key), (char*)((struct info*)q->data)->value);
            q = q->next;
        }
    }
    printf("\n");*/
	return server_retrieve(main->servers[*server_id], key);
}

int insert_sorted(struct server_info *v, int n, unsigned int x)
{
    int i = 0;
    unsigned int hashed_id = hash_function_servers(&x);
    while (i < n && v[i].hashed_id < hashed_id)
        i++;
    for (int j = n; j > i; j--)
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
    //printf("\n");
    main->servers[server_id] = init_server_memory();
    for (unsigned int k = 0; k < 3; k++)
    {
        temp_id = k*100000 + server_id;
        pos = insert_sorted(main->ids, main->num_labels, temp_id);
        hashed_id = hash_function_servers(&temp_id);
        main->num_labels += 1;
        hashtable_t *ht_to_check = main->servers[main->ids[(pos + 1) % main->num_labels].id % 100000]->ht;
        hashtable_t *ht_new = main->servers[main->ids[pos].id % 100000]->ht;
        if (server_id != main->ids[(pos + 1) % main->num_labels].id % 100000)
        {
            int key_count;
            char **keys = ht_get_keys(ht_to_check, &key_count);
            /*printf("%d's keys:\n", main->ids[(pos + 1) % main->num_labels].id % 100000);
            for (int i = 0; i < key_count; i++)
                printf("%s(%010u)\n", keys[i], hash_function_key(keys[i]));
            printf("hashring:\n");
            for (int i = 0; i < main->num_labels; i++)
                printf("%010u ", main->ids[i].hashed_id);
            printf("\n");*/
            for (int i = 0; i < key_count; i++)
            {
                unsigned int hashed_key = hash_function_key(keys[i]);
                if (pos == 0)
                {
                    if (hashed_key > main->ids[main->num_labels - 1].hashed_id || hashed_key < hashed_id)
                    {
                        void *value = ht_get(ht_to_check, keys[i]);
                        //printf("%d(%010u) -> %d(%010u): %010u %s\n", main->ids[(pos + 1) % main->num_labels].id, main->ids[(pos + 1) % main->num_labels].hashed_id, main->ids[pos].id, main->ids[pos].hashed_id, hashed_key, (char*)value);
                        ht_put(ht_new, keys[i], strlen(keys[i]) + 1, value, strlen((char*)value) + 1);
                        ht_remove_entry(ht_to_check, keys[i]);
                    }
                }
                else if (hashed_key < hashed_id && main->ids[(pos - 1) % main->num_labels].hashed_id < hashed_key)
                {
                    void *value = ht_get(ht_to_check, keys[i]);
                    //printf("%d(%010u) -> %d(%010u): %010u %s\n", main->ids[(pos + 1) % main->num_labels].id, main->ids[(pos + 1) % main->num_labels].hashed_id, main->ids[pos].id, main->ids[pos].hashed_id, hashed_key, (char*)value);
                    ht_put(ht_new, keys[i], strlen(keys[i]) + 1, value, strlen((char*)value) + 1);
                    ht_remove_entry(ht_to_check, keys[i]);
                }
            }
            for (int i = 0; i < key_count; i++)
                free(keys[i]);
            free(keys);
        }
    }
    //printf("\n");
}


void loader_remove_server(load_balancer* main, int server_id) {
    int pos;
    unsigned int hashed_id, temp_id, rem_id, hashed_key;
    hashtable_t *ht_to_check = main->servers[server_id % 100000]->ht;
    for (unsigned int k = 0; k < 3; k++)
    {
        temp_id = k*100000 + server_id;
        hashed_id = hash_function_servers(&temp_id);
        pos = bin_search(main->ids, main->num_labels, hashed_id);
        rem_id = remove_nth(main->ids, main->num_labels, pos);
        main->num_labels -= 1;
        hashtable_t *ht_new = main->servers[rem_id % 100000]->ht;
        if (rem_id % 100000 != server_id)
        {
            int key_count;
            char **keys = ht_get_keys(ht_to_check, &key_count);
            for (int i = 0; i < key_count; i++)
            {
                hashed_key = hash_function_key(keys[i]);
                if ((hashed_key < hashed_id && hashed_key > main->ids[(pos - 1) % main->num_labels].hashed_id) || pos == main->num_labels)
                {
                    void *value = ht_get(ht_to_check, keys[i]);
                    ht_put(ht_new, keys[i], strlen(keys[i]) + 1, value, strlen((char*)value) + 1);
                    ht_remove_entry(ht_to_check, keys[i]);
                }
            }
            for (int i = 0; i < key_count; i++)
                free(keys[i]);
            free(keys);
        }
    }
    free_server_memory(main->servers[server_id]);
    main->servers[server_id] = NULL;
}

void free_load_balancer(load_balancer* main) {
    for (int i = 0; i < 100000; i++)
        if (main->servers[i])
            free_server_memory(main->servers[i]);
    free(main->servers);
    free(main);
}
