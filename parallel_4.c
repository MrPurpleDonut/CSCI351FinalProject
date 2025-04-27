#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <omp.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#define MAX_CITIES       100
#define MAX_NAME_LENGTH  20

// Holds statistics for one city.
typedef struct {
char   name[MAX_NAME_LENGTH];
double lowest;
double highest;
double sum;
int    count;
} City;

int compareCities(const void *a, const void *b) {
const City *cityA = (const City *) a;
const City *cityB = (const City *) b;
return strcmp(cityA->name, cityB->name);
}

// Merge data from "src" into "dest".
static void mergeCityData(City *dest, const City *src) {
if (dest->count == 0) {
// If dest is unused, copy everything over
*dest = *src;
} else {
// If dest already has some data, combine
dest->count += src->count;
dest->sum   += src->sum;
if (src->lowest < dest->lowest) {
dest->lowest = src->lowest;
}
if (src->highest > dest->highest) {
dest->highest = src->highest;
}
}
}

int main(int argc, char *argv[]) {
if (argc < 2) {
fprintf(stderr, "Usage: %s <filename>\n", argv[0]);
return 1;
}

// 1) Open file
int fd = open(argv[1], O_RDONLY);
if (fd < 0) {
    perror("open");
    return 1;
}

// 2) Get file size
struct stat sb;
if (fstat(fd, &sb) < 0) {
    perror("fstat");
    close(fd);
    return 1;
}
size_t fsize = sb.st_size;
if (fsize == 0) {
    fprintf(stderr, "File is empty.\n");
    close(fd);
    return 1;
}

// 3) Map the file into memory (read-only)
char *fileData = mmap(NULL, fsize, PROT_READ, MAP_PRIVATE, fd, 0);
if (fileData == MAP_FAILED) {
    perror("mmap");
    close(fd);
    return 1;
}
close(fd);

// Start timing
double startTime = omp_get_wtime();

// 4) Find the start offsets of every line
size_t capacity = 10000; // arbitrary initial guess
size_t *offsets = (size_t*)malloc(capacity * sizeof(size_t));
if (!offsets) {
    fprintf(stderr, "Malloc error\n");
    munmap(fileData, fsize);
    return 1;
}

size_t lineCount = 0;
// First line starts at offset 0
offsets[lineCount++] = 0;

for (size_t i = 0; i < fsize; i++) {
    if (fileData[i] == '\n') {
        size_t nextPos = i + 1;
        if (nextPos < fsize) {
            // Grow offsets array if needed
            if (lineCount >= capacity) {
                capacity *= 2;
                offsets = realloc(offsets, capacity * sizeof(size_t));
                if (!offsets) {
                    fprintf(stderr, "Realloc error\n");
                    munmap(fileData, fsize);
                    return 1;
                }
            }
            offsets[lineCount++] = nextPos;
        }
    }
}

// 5) Prepare per-thread array of City structures
int nThreads = 1;
#pragma omp parallel
{
    nThreads = omp_get_num_threads();
}
City *cityDataPerThread = calloc(nThreads * MAX_CITIES, sizeof(City));
if (!cityDataPerThread) {
    fprintf(stderr, "Calloc error\n");
    munmap(fileData, fsize);
    free(offsets);
    return 1;
}

// 6) Parallel parsing:
//    - Each thread processes a range of lines from [startLine, endLine).
//    - Use thread-private arrays for city statistics to avoid lock contention.

#pragma omp parallel 
{
    int tid = omp_get_thread_num();
    // Local pointer to this thread's portion of cityDataPerThread
    City *localCities = cityDataPerThread + tid * MAX_CITIES;

    // Compute chunk of lines for this thread
    size_t chunkSize  = lineCount / nThreads;
    size_t startLine  = tid * chunkSize;
    size_t endLine    = (tid == nThreads - 1) ? lineCount : (tid + 1)*chunkSize;

    // Parse each line in [startLine, endLine)
    for (size_t i = startLine; i < endLine; i++) {
        size_t offset = offsets[i];
        if (offset >= fsize) {
            // Shouldn't happen, but just a safeguard
            continue;
        }

        // Find end of this line
        size_t lineEnd = (i + 1 < lineCount) ? offsets[i + 1] - 1 : fsize;
        if (lineEnd <= offset) {
            // Empty line or something odd
            continue;
        }

        size_t length = lineEnd - offset;
        if (length == 0) {
            continue; // blank line
        }

        // Copy the line into a temporary buffer
        // to safely tokenize (because fileData isn't null-terminated).
        char lineBuf[128];
        if (length >= sizeof(lineBuf)) {
            // Line too long, skip or truncate
            continue;
        }
        memcpy(lineBuf, fileData + offset, length);
        lineBuf[length] = '\0';

        // Parse "CityName;value"
        char *name   = strtok(lineBuf, ";");
        char *valStr = strtok(NULL, ";");
        if (!name || !valStr) {
            // Malformed line
            continue;
        }
        double val = atof(valStr);

        // Insert or update city stats in localCities
        int updated = 0;
        for (int c = 0; c < MAX_CITIES; c++) {
            if (localCities[c].count == 0) {
                // We found an empty slot, set it
                strncpy(localCities[c].name, name, MAX_NAME_LENGTH - 1);
                localCities[c].name[MAX_NAME_LENGTH - 1] = '\0';
                localCities[c].lowest = val;
                localCities[c].highest= val;
                localCities[c].sum    = val;
                localCities[c].count  = 1;
                updated = 1;
                break;
            }
            // If we match the city name, update
            if (strcmp(localCities[c].name, name) == 0) {
                localCities[c].count++;
                localCities[c].sum += val;
                if (val < localCities[c].lowest) {
                    localCities[c].lowest = val;
                }
                if (val > localCities[c].highest) {
                    localCities[c].highest = val;
                }
                updated = 1;
                break;
            }
        }
        // If updated == 0 here, we ran out of space in localCities (MAX_CITIES).
        // In a real scenario, you might switch to a hash map or dynamic structure.
    }
} // end of parallel region

// 7) Merge all thread-local city data into one global array
City globalCities[MAX_CITIES];
memset(globalCities, 0, sizeof(globalCities));

for (int t = 0; t < nThreads; t++) {
    City *localCities = cityDataPerThread + t * MAX_CITIES;
    for (int c = 0; c < MAX_CITIES; c++) {
        if (localCities[c].count == 0) {
            continue;
        }
        // Either find the city in globalCities or a free slot
        for (int g = 0; g < MAX_CITIES; g++) {
            if (globalCities[g].count == 0) {
                // Use this slot
                globalCities[g] = localCities[c];
                break;
            }
            if (strcmp(globalCities[g].name, localCities[c].name) == 0) {
                mergeCityData(&globalCities[g], &localCities[c]);
                break;
            }
        }
    }
}

// 8) Sort, print results
qsort(globalCities, MAX_CITIES, sizeof(City), compareCities);
for (int i = 0; i < MAX_CITIES; i++) {
    if (globalCities[i].count == 0) {
        continue;
    }
    double avg = globalCities[i].sum / globalCities[i].count;
    printf("%s: %.1f / %.1f / %.1f\n",
           globalCities[i].name, globalCities[i].lowest, avg, globalCities[i].highest);
}

double endTime = omp_get_wtime();
printf("Time: %f seconds\n", endTime - startTime);

// 9) Cleanup
munmap(fileData, fsize);
free(offsets);
free(cityDataPerThread);

return 0;
}