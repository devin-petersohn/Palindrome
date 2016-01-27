#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define BUFFER_LINES_IN 100

int main(int argc, char **argv) {

	FILE* dirtyInput = fopen(argv[1], "r");
	if(!dirtyInput) {
		fprintf(stderr, "%s\n", "Unable to open file");
		return -1;
	}
	
	char *output_filename = malloc(sizeof(char) * BUFFER_LINES_IN);
	strcpy(output_filename, argv[1]);
	strcat(output_filename, ".clean");
	FILE* cleanOutput = fopen(output_filename, "w");
	free(output_filename);

	if(!cleanOutput) {
		fprintf(stderr, "%s\n", "File error. Unable to create.");
		return -1;	
	}
	
	char* temporary_input_string = malloc(sizeof(char) * BUFFER_LINES_IN);
	int line_counter_for_total_size = 0;
	int current_size = 0;

	while(fgets(temporary_input_string, BUFFER_LINES_IN, dirtyInput) != NULL){
		if(temporary_input_string[strlen(temporary_input_string)-1] == '\n'){
			temporary_input_string[strlen(temporary_input_string)-1] = '\0';
			if(temporary_input_string[0] == '>') {
				if(current_size != 0) fprintf(cleanOutput, "\n");
				current_size += 1;
				fprintf(cleanOutput, "%s_%sBREAK_HERE_PALINDROME", argv[1], temporary_input_string);
				current_size = 1;
			} else {
				current_size += 1;
				fprintf(cleanOutput, "%s", temporary_input_string);
			}
		} else{
			free(temporary_input_string);
			fprintf(stderr, "%s\n", "Lines too long.");
			return -1;
		}
	}

	free(temporary_input_string);
	return 0;


}

