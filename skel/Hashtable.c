// Copyright 2021 Condrea Tudor Daniel

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "utils.h"

#include "Hashtable.h"

#define MAX_BUCKET_SIZE 64

/*
 * Functii de comparare a cheilor:
 */
int
compare_function_ints(void *a, void *b)
{
	int int_a = *((int *)a);
	int int_b = *((int *)b);

	if (int_a == int_b) {
		return 0;
	} else if (int_a < int_b) {
		return -1;
	} else {
		return 1;
	}
}

int
compare_function_strings(void *a, void *b)
{
	char *str_a = (char *)a;
	char *str_b = (char *)b;

	return strcmp(str_a, str_b);
}

/*
 * Functii de hashing:
 */
unsigned int
hash_function_int(void *a)
{
	/*
	 * Credits: https://stackoverflow.com/a/12996028/7883884
	 */
	unsigned int uint_a = *((unsigned int *)a);

	uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
	uint_a = ((uint_a >> 16u) ^ uint_a) * 0x45d9f3b;
	uint_a = (uint_a >> 16u) ^ uint_a;
	return uint_a;
}

unsigned int
hash_function_string(void *a)
{
	/*
	 * Credits: http://www.cse.yorku.ca/~oz/hash.html
	 */
	unsigned char *puchar_a = (unsigned char*) a;
	unsigned long hash = 5381;
	int c;

	while ((c = *puchar_a++))
		hash = ((hash << 5u) + hash) + c; /* hash * 33 + c */

	return hash;
}

/*
 * Functie apelata dupa alocarea unui hashtable pentru a-l initializa.
 * Trebuie alocate si initializate si listele inlantuite.
 */
hashtable_t *
ht_create(unsigned int hmax, unsigned int (*hash_function)(void*),
		int (*compare_function)(void*, void*))
{
	hashtable_t *table = malloc(sizeof(hashtable_t));
	DIE(!table, "ht malloc failure");
	table->hmax = hmax;
	table->size = 0;
	table->compare_function = compare_function;
	table->hash_function = hash_function;
	table->buckets = malloc(hmax * sizeof(linked_list_t*));
	DIE(!table->buckets, "buckets malloc failure");
	for (unsigned int i = 0; i < hmax; i++)
		table->buckets[i] = ll_create(sizeof(struct info));
	return table;
}

/*
 * Atentie! Desi cheia este trimisa ca un void pointer (deoarece nu se impune tipul ei), in momentul in care
 * se creeaza o noua intrare in hashtable (in cazul in care cheia nu se gaseste deja in ht), trebuie creata o copie
 * a valorii la care pointeaza key si adresa acestei copii trebuie salvata in structura info asociata intrarii din ht.
 * Pentru a sti cati octeti trebuie alocati si copiati, folositi parametrul key_size_bytes.
 *
 * Motivatie:
 * Este nevoie sa copiem valoarea la care pointeaza key deoarece dupa un apel put(ht, key_actual, value_actual),
 * valoarea la care pointeaza key_actual poate fi alterata (de ex: *key_actual++). Daca am folosi direct adresa
 * pointerului key_actual, practic s-ar modifica din afara hashtable-ului cheia unei intrari din hashtable.
 * Nu ne dorim acest lucru, fiindca exista riscul sa ajungem in situatia in care nu mai stim la ce cheie este
 * inregistrata o anumita valoare.
 */
void
ht_put(hashtable_t *ht, void *key, unsigned int key_size,
	void *value, unsigned int value_size)
{
	unsigned int bucketIndex = ht->hash_function(key);
	bucketIndex %= ht->hmax;
	linked_list_t *bucket = ht->buckets[bucketIndex];
	ll_node_t *q = bucket->head;
	while(q != NULL)
	{
		if (ht->compare_function(((struct info*)q->data)->key, key) == 0)
		{
			void *new_ptr = realloc(((struct info*)q->data)->value, value_size);
			DIE(!new_ptr, "ht rewrite realloc failure");
			((struct info*)q->data)->value = new_ptr;
			memcpy(((struct info*)q->data)->value, value, value_size);
			return;
		}
		q = q->next;
	}
	struct info adr;
	adr.key = malloc(key_size);
	DIE(!adr.key, "ht key malloc failure");
	adr.value = malloc(value_size);
	DIE(!adr.value, "ht value malloc failure");
	memcpy(adr.key, key, key_size);
	memcpy(adr.value, value, value_size);
	ll_add_nth_node(bucket, bucket->size, &adr);
	ht->size++;
}

void *
ht_get(hashtable_t *ht, void *key)
{
	unsigned int bucketIndex = ht->hash_function(key) % ht->hmax;
	for (ll_node_t *q = ht->buckets[bucketIndex]->head; q != NULL; q = q->next)
		if (ht->compare_function(((struct info *)q->data)->key, key) == 0)
			return ((struct info*)q->data)->value;
	return NULL;
}

/*
 * Functie care intoarce:
 * 1, daca pentru cheia key a fost asociata anterior o valoare in hashtable folosind functia put
 * 0, altfel.
 */
int
ht_has_key(hashtable_t *ht, void *key)
{
	unsigned int bucketIndex = ht->hash_function(key) % ht->hmax;
	for (ll_node_t *q = ht->buckets[bucketIndex]->head; q != NULL; q = q->next)
		if (ht->compare_function(((struct info *)q->data)->key, key) == 0)
			return 1;
	return 0;
}

/*
 * Procedura care elimina din hashtable intrarea asociata cheii key.
 * Atentie! Trebuie avuta grija la eliberarea intregii memorii folosite pentru o intrare din hashtable (adica memoria
 * pentru copia lui key --vezi observatia de la procedura put--, pentru structura info si pentru structura Node din
 * lista inlantuita).
 */
void
ht_remove_entry(hashtable_t *ht, void *key)
{
	unsigned int bucketIndex = ht->hash_function(key) % ht->hmax;
	int count = 0;
	ll_node_t *q = ht->buckets[bucketIndex]->head;
	while(q != NULL)
	{
		if (ht->compare_function(((struct info *)q->data)->key, key) == 0)
		{
			free(((struct info *)q->data)->key);
			free(((struct info *)q->data)->value);
			ll_node_t *p = ll_remove_nth_node(ht->buckets[bucketIndex], count);
			free(p->data);
			free(p);
			ht->size--;
			return;
		}
		q = q->next;
		count++;
	}
}

/*
 * Procedura care elibereaza memoria folosita de toate intrarile din hashtable, dupa care elibereaza si memoria folosita
 * pentru a stoca structura hashtable.
 */
void
ht_free(hashtable_t *ht)
{
	for (unsigned int i = 0; i < ht->hmax; i++)
	{
		ll_node_t *q = ht->buckets[i]->head;
		while (q != NULL)
		{
			free(((struct info*)q->data)->key);
			free(((struct info*)q->data)->value);
			q = q->next;
		}
		ll_free(&ht->buckets[i]);
		ht->buckets[i] = NULL;
	}
	free(ht->buckets);
	ht->buckets = NULL;
	free(ht);
	ht = NULL;
}

unsigned int
ht_get_size(hashtable_t *ht)
{
	if (ht == NULL)
		return 0;
	return ht->size;
}

unsigned int
ht_get_hmax(hashtable_t *ht)
{
	if (ht == NULL)
		return 0;

	return ht->hmax;
}

char **
ht_get_keys(hashtable_t *ht, int *n)
{
	*n = 0;
	char **keys = malloc(sizeof(char*)), *extracted_key;
	DIE(!keys, "ht key retrieval malloc failure");
	for (unsigned int i = 0; i < ht->hmax; i++)
		for (ll_node_t *q = ht->buckets[i]->head; q != NULL; q = q->next)
		{
			char **new_ptr = realloc(keys, (*n + 1)*sizeof(char*));
			DIE(!new_ptr, "ht key retrieval resize realloc failure");
			keys = new_ptr;
			extracted_key = ((struct info*)q->data)->key;
			keys[*n] = malloc(strlen(extracted_key) + 1);
			DIE(!keys[*n], "ht key retrieval new key malloc failure");
			memcpy(keys[*n], extracted_key, strlen(extracted_key) + 1);
			*n += 1;
		}
	return keys;
}
