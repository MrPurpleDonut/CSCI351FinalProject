#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>

#define MAX_LINE_LEN 128
#define QUEUE_SIZE 1024

static int NUM_CITIES = 100;
static int NUM_THREADS = 8;

typedef struct {
    char name[20];
    double lowest;
    double highest;
    double sum;
    int count;
} City;

// Thread-safe queue for lines
typedef struct {
    char lines[QUEUE_SIZE][MAX_LINE_LEN];
    int head;
    int tail;
    int done;
    omp_lock_t lock;
} LineQueue;

void initQueue(LineQueue *q) {
    q->head = 0;
    q->tail = 0;
    q->done = 0;
    omp_init_lock(&q->lock);
}

void destroyQueue(LineQueue *q) {
    omp_destroy_lock(&q->lock);
}

int enqueue(LineQueue *q, const char *line) {
    int success = 0;
    omp_set_lock(&q->lock);
    int next = (q->tail + 1) % QUEUE_SIZE;
    if (next != q->head) { // Queue not full
        strncpy(q->lines[q->tail], line, MAX_LINE_LEN);
        q->lines[q->tail][MAX_LINE_LEN-1] = '\0';
        q->tail = next;
        success = 1;
    }
    omp_unset_lock(&q->lock);
    return success;
}

int dequeue(LineQueue *q, char *lineOut) {
    int success = 0;
    omp_set_lock(&q->lock);
    if (q->head != q->tail) { // Queue not empty
        strncpy(lineOut, q->lines[q->head], MAX_LINE_LEN);
        lineOut[MAX_LINE_LEN-1] = '\0';
        q->head = (q->head + 1) % QUEUE_SIZE;
        success = 1;
    }
    omp_unset_lock(&q->lock);
    return success;
}

int compareCities(const void *a, const void *b) {
    City *cityA = (City *)a;
    City *cityB = (City *)b;
    return strcmp(cityA->name, cityB->name);
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        fprintf(stderr, "File name needed\n");
        return 1;
    }

    FILE *file = fopen(argv[1], "r");
    if (file == NULL) {
        fprintf(stderr, "Error reading file\n");
        return 1;
    }

    double startTime = omp_get_wtime();
    omp_set_num_threads(NUM_THREADS);

    City cities[NUM_CITIES];
    memset(cities, 0, sizeof(cities));

    LineQueue queue;
    initQueue(&queue);

    #pragma omp parallel
    {
        int id = omp_get_thread_num();
        if (id == 0) {
            // Reader thread
            char buffer[MAX_LINE_LEN];
            while (fgets(buffer, sizeof(buffer), file)) {
                buffer[strcspn(buffer, "\n")] = 0; // remove newline
                // Wait until the line is queued
                while (!enqueue(&queue, buffer)) {
                    // Busy-wait if queue is full
                    #pragma omp flush
                }
            }
            fclose(file);
            omp_set_lock(&queue.lock);
            queue.done = 1; // signal done
            omp_unset_lock(&queue.lock);
        } else {
            // Worker threads
            City localCities[NUM_CITIES];
            memset(localCities, 0, sizeof(localCities));

            char line[MAX_LINE_LEN];
            while (1) {
                int gotLine = dequeue(&queue, line);
                if (!gotLine) {
                    omp_set_lock(&queue.lock);
                    int finished = queue.done && (queue.head == queue.tail);
                    omp_unset_lock(&queue.lock);
                    if (finished) break;
                    continue; // Nothing now, try again
                }

                char* saveptr;
                char* name = strtok_r(line, ";", &saveptr);
                double val = atof(strtok_r(NULL, ";", &saveptr));

                for (int i = 0; i < NUM_CITIES; i++) {
                    if (localCities[i].count == 0) {
                        snprintf(localCities[i].name, sizeof(localCities[i].name), "%s", name);
                        localCities[i].count = 1;
                        localCities[i].lowest = val;
                        localCities[i].highest = val;
                        localCities[i].sum = val;
                        break;
                    }
                    if (strcmp(localCities[i].name, name) == 0) {
                        localCities[i].count++;
                        localCities[i].sum += val;
                        if (val < localCities[i].lowest) localCities[i].lowest = val;
                        if (val > localCities[i].highest) localCities[i].highest = val;
                        break;
                    }
                }
            }

            // Merge localCities into global cities
            #pragma omp critical
            {
                for (int i = 0; i < NUM_CITIES; i++) {
                    if (localCities[i].count == 0) continue;
                    int found = 0;
                    for (int j = 0; j < NUM_CITIES; j++) {
                        if (cities[j].count == 0) {
                            snprintf(cities[j].name, sizeof(cities[j].name), "%s", localCities[i].name);
                            cities[j].count = localCities[i].count;
                            cities[j].lowest = localCities[i].lowest;
                            cities[j].highest = localCities[i].highest;
                            cities[j].sum = localCities[i].sum;
                            found = 1;
                            break;
                        }
                        if (strcmp(cities[j].name, localCities[i].name) == 0) {
                            cities[j].count += localCities[i].count;
                            cities[j].sum += localCities[i].sum;
                            if (localCities[i].lowest < cities[j].lowest) cities[j].lowest = localCities[i].lowest;
                            if (localCities[i].highest > cities[j].highest) cities[j].highest = localCities[i].highest;
                            found = 1;
                            break;
                        }
                    }
                    if (!found) {
                        fprintf(stderr, "Error: too many cities\n");
                        exit(1);
                    }
                }
            }
        }
    }

    destroyQueue(&queue);

    qsort(cities, NUM_CITIES, sizeof(City), compareCities);

    for (int i = 0; i < NUM_CITIES; i++) {
        if (cities[i].count > 0) {
            printf("%s: %.1f/%.1f/%.1f\n", cities[i].name,
                cities[i].lowest,
                cities[i].sum / cities[i].count,
                cities[i].highest);
        }
    }

    double endTime = omp_get_wtime();
    printf("Time: %f seconds\n", endTime - startTime);

    return 0;
}
