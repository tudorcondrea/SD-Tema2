/* Copyright 2021 <Condrea Tudor Daniel> */
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "load_balancer.h"
#include "utils.h"

struct server_info {
    unsigned int id, hashed_id;
};

struct load_balancer {
	struct server_info ids[300000];  // retine toate etichetele
                                     // ordonate dupa hash
    server_memory **servers;  // array de pointere la servere
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
    DIE(!bal, "load balancer malloc failure");
    bal->servers = calloc(100000, sizeof(server_memory*));
    DIE(!bal->servers, "servers array calloc failure");
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
    // incadreaza hashul intre 2 iduri si returneaza pe cel din dreapta
    int pos = bin_search(main->ids, main->num_labels, key_hashed);
    pos %= main->num_labels;
    // ne trebuie id-ul serverului care retine perechea
	*server_id = main->ids[pos].id;
    *server_id %= 100000;
    server_store(main->servers[*server_id], key, value);
}


char* loader_retrieve(load_balancer* main, char* key, int* server_id) {
	unsigned int key_hashed = hash_function_key(key);
	int pos = bin_search(main->ids, main->num_labels, key_hashed);
    pos %= main->num_labels;
	*server_id = main->ids[pos].id;
    *server_id %= 100000;
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
    int pos, next_id, prev_id;
    unsigned int hashed_id, temp_id;
    main->servers[server_id] = init_server_memory();
    // pentru fiecare server inserat, sunt adaugate 3 etichete
    for (unsigned int k = 0; k < 3; k++)
    {
        temp_id = k*100000 + server_id;
        pos = insert_sorted(main->ids, main->num_labels, temp_id);
        hashed_id = hash_function_servers(&temp_id);
        main->num_labels += 1;
        next_id = main->ids[(pos + 1) % main->num_labels].id % 100000;
        if (server_id != next_id)
        {
            int key_count;
            char **keys = server_get_keys(main->servers[next_id], &key_count);
            prev_id = (pos - 1) % main->num_labels;
            for (int i = 0; i < key_count; i++)
            {
                unsigned int hashed_key = hash_function_key(keys[i]);
                /**trebuie sa dau handle separat pentru edge case-ul
                 * in care serverul inserat e pe pozitia 0
                 * pentru ca C-ul nu a trecut clasa a 11-a
                 * si considera ca -1 % n = -1 si nu n - 1
                 */
                if (pos == 0)
                {
                    if (hashed_key > main->ids[main->num_labels - 1].hashed_id
                        || hashed_key < hashed_id)
                    {
                        void *value = server_retrieve(main->servers[next_id],
                                                      keys[i]);
                        server_store(main->servers[server_id], keys[i], value);
                        server_remove(main->servers[next_id], keys[i]);
                    }
                }
                else if (hashed_key < hashed_id &&
                         main->ids[prev_id].hashed_id < hashed_key)
                {
                    void *value = server_retrieve(main->servers[next_id],
                                                  keys[i]);
                    server_store(main->servers[server_id], keys[i], value);
                    server_remove(main->servers[next_id], keys[i]);
                }
            }
            for (int i = 0; i < key_count; i++)
                free(keys[i]);
            free(keys);
        }
    }
}


void loader_remove_server(load_balancer* main, int server_id) {
    int pos, temp_id, rem_id, prev_pos;
    unsigned int hashed_id, hashed_key;
    for (unsigned int k = 0; k < 3; k++)
    {
        temp_id = k*100000 + server_id;
        hashed_id = hash_function_servers(&temp_id);
        pos = bin_search(main->ids, main->num_labels, hashed_id);
        // rem_id e eticheta de dupa cea scoasa
        rem_id = remove_nth(main->ids, main->num_labels, pos) % 100000;
        main->num_labels -= 1;
        if (rem_id != server_id)
        {
            int key_count;
            char **keys = server_get_keys(main->servers[server_id], &key_count);
            for (int i = 0; i < key_count; i++)
            {
                hashed_key = hash_function_key(keys[i]);
                prev_pos = (pos - 1) % main->num_labels;
                if ((hashed_key < hashed_id &&
                    hashed_key > main->ids[prev_pos].hashed_id)
                    || pos == main->num_labels)
                {
                    void *value = server_retrieve(main->servers[server_id],
                                                  keys[i]);
                    server_store(main->servers[rem_id], keys[i], value);
                    server_remove(main->servers[server_id], keys[i]);
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
