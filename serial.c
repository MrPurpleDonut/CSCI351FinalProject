#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include <string.h>


static int NUM_CITIES = 100;


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
	char buffer[25];//Max size for city name is 16
	City cities[NUM_CITIES];
	for(int i = 0; i < NUM_CITIES; i++){
		cities[i].count = 0;
	}
	while(fgets(buffer, sizeof(buffer), file) != NULL){
		buffer[strcspn(buffer, "\n")] = 0;
		char* name = strtok(buffer, ";");
		double val = atof(strtok(NULL, ";"));
		for(int i = 0; i < NUM_CITIES; i++){
			if(cities[i].count == 0){
				snprintf(cities[i].name, sizeof(cities[i].name), "%s", name);
				cities[i].count = 1;
				cities[i].lowest = val;
				cities[i].highest = val;
				cities[i].sum = val;
				break;
			}
			if(strcmp(cities[i].name, name) == 0){
				cities[i].count++;
				cities[i].sum+=val;
				if(val < cities[i].lowest){
					cities[i].lowest = val;
				}
				if(val > cities[i].highest){
					cities[i].highest = val;
				}
				break;
			}
		}
	}

	qsort(cities, NUM_CITIES, sizeof(City), compareCities);
	for(int i = 0; i < NUM_CITIES; i++){
		printf("%s: %.1f/%.1f/%.1f\n", cities[i].name, cities[i].lowest, cities[i].sum/cities[i].count, cities[i].highest);
	}
	double endTime = omp_get_wtime();

	printf("Time: %f seconds\n", endTime-startTime);
	fclose(file);
}


