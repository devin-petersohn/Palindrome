#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define BUFFER_LINES_IN 100

int main(int argc, char **argv) {

	FILE* dirtyInput = fopen(argv[1], "r");
	FILE* cleanOutput = fopen("cleanOutput.txt", "w");

	char* temporary_input_string = malloc(sizeof(char) * BUFFER_LINES_IN);
	
	int line_counter_for_total_size = 0;

	int current_size = 0;

	while(fgets(temporary_input_string, 100, dirtyInput) != NULL){
		if(temporary_input_string[strlen(temporary_input_string)-1] == '\n')
			temporary_input_string[strlen(temporary_input_string)-1] = '\0';
		if(temporary_input_string[0] == '>') {
			if(current_size != 0) fprintf(cleanOutput, "\n");
			current_size += 1;
			fprintf(cleanOutput, "%s\n", temporary_input_string);
			current_size = 1;
		} else {
			current_size += 1;
			fprintf(cleanOutput, "%s", temporary_input_string);
		}
	}



}

