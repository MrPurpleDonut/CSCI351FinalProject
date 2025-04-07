#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include <string.h>


static int NUM_CITIES = 100;
static int NUM_THREADS = 8;

typedef struct {
	char name[20];
	double lowest;
	double highest;
	double sum;
	int count;
} City;

int compareCities(const void *a, const void *b){
	City *cityA = (City *)a;
	City *cityB = (City *)b;
	return strcmp(cityA->name, cityB->name);
}

int main(int argc, char* argv[]) {
	if (argc < 2){
		fprintf(stderr, "File name needed\n");
		return 1;
	}

	FILE *file = fopen(argv[1], "r"); 
	if (file == NULL){
		fprintf(stderr, "Error reading file");
		return 1;
	}

	double startTime = omp_get_wtime();
	fseek(file, 0, SEEK_END);
	long fileSize = ftell(file);
	fclose(file);
	long chunkSize = fileSize / NUM_THREADS;
	omp_set_num_threads(NUM_THREADS);

	City cities[NUM_CITIES];
	for(int i = 0; i < NUM_CITIES; i++){
		cities[i].count = 0;
	}
	#pragma omp parallel
	{
		City localCities[NUM_CITIES]; 
		for(int i = 0; i < NUM_CITIES; i++){
			localCities[i].count = 0;
		}
		int id = omp_get_thread_num();
		long start = id*chunkSize;
		long end = (id+1)*chunkSize;
		FILE *fp = fopen(argv[1], "r");
		fseek(fp, start, SEEK_SET);
		char c;
		if(id != 0){
			while((c = fgetc(fp)) != '\n' && c != EOF){
				start++;
			}
		}


		char buffer[25];//Max size for city name is 16
		while(ftell(fp) < end && fgets(buffer, sizeof(buffer), fp) != NULL){
			buffer[strcspn(buffer, "\n")] = 0;
			char* saveptr;
			char* name = strtok_r(buffer, ";", &saveptr);
			double val = atof(strtok_r(NULL, ";", &saveptr));
			for(int i = 0; i < NUM_CITIES; i++){
				if(localCities[i].count == 0){
					snprintf(localCities[i].name, sizeof(localCities[i].name), "%s", name);
					localCities[i].count = 1;
					localCities[i].lowest = val;
					localCities[i].highest = val;
					localCities[i].sum = val;
					break;
				}
				if(strcmp(localCities[i].name, name) == 0){
					localCities[i].count++;
					localCities[i].sum+=val;
					if(val < localCities[i].lowest){
						localCities[i].lowest = val;
					}
					if(val > localCities[i].highest){
						localCities[i].highest = val;
					}
					break;
				}
			}
		
		}
		fclose(fp);
		qsort(localCities, NUM_CITIES, sizeof(City), compareCities);
		#pragma omp critical
		{
			for(int i = 0; i < NUM_CITIES; i++){
				if(cities[i].count == 0){
					snprintf(cities[i].name, sizeof(cities[i].name), "%s", localCities[i].name);
					cities[i].count = localCities[i].count;
					cities[i].lowest = localCities[i].lowest;
					cities[i].highest = localCities[i].highest;
					cities[i].sum = localCities[i].sum;
				}else{
					cities[i].count += localCities[i].count;
					cities[i].sum += localCities[i].sum;
					if(localCities[i].lowest < cities[i].lowest){
						cities[i].lowest = localCities[i].lowest;
					}
					if(localCities[i].highest > cities[i].highest){
						cities[i].highest = localCities[i].highest;
					}
				}
			}

		}
		
	}

	for(int i = 0; i < NUM_CITIES; i++){
		printf("%s: %.1f/%.1f/%.1f\n", cities[i].name, cities[i].lowest, cities[i].sum/cities[i].count, cities[i].highest);
	}
	double endTime = omp_get_wtime();

	printf("Time: %f seconds\n", endTime-startTime);
}


