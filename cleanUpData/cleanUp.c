#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>
#include <math.h>

#define BUFFER_LINES_IN 1000
#define MAX_NUMBER_OF_LINES 1000000

int main(int argc, char **argv) {

	FILE* dirtyInput = fopen(argv[1], "r");
	if(!dirtyInput) {
		fprintf(stderr, "%s\n", "Unable to open file");
		return -1;
	}
	
	char *output_filename = malloc(sizeof(char) * BUFFER_LINES_IN);
	strcpy(output_filename, "../intermediate_data/");
	strcat(output_filename, basename(argv[1]));
	strcat(output_filename, ".clean");
	FILE* cleanOutput = fopen(output_filename, "w");
	FILE* list_of_files = fopen("../intermediate_data/list_of_files.clean", "w");
	fprintf(list_of_files, "%s\n", output_filename);
	char buffer[1024];
	char previous_name_buffer[1024];
	int file_count = 0;

	if(!cleanOutput) {
		fprintf(stderr, "%s\n", "File error. Unable to create.");
		return -1;	
	}
	
	char* temporary_input_string = malloc(sizeof(char) * BUFFER_LINES_IN);
	int current_size = 0;

	while(fgets(temporary_input_string, BUFFER_LINES_IN, dirtyInput) != NULL){
		if(temporary_input_string[strlen(temporary_input_string)-1] == '\n'){
			temporary_input_string[strlen(temporary_input_string)-1] = '\0';
			if(temporary_input_string[0] == '>') {
				strcpy(previous_name_buffer, temporary_input_string);
				if(current_size > MAX_NUMBER_OF_LINES) {
					file_count+= 1;
					fclose(cleanOutput);
					output_filename[strlen(output_filename) - 7 - (int) log10(file_count)] = '\0';
					sprintf(buffer, "%d", file_count);
					strcat(output_filename, buffer);
					strcat(output_filename, ".clean");
					cleanOutput = fopen(output_filename, "w");
					fprintf(list_of_files, "%s\n", output_filename);
					current_size = 0;
					
				}
				if(current_size != 0) fprintf(cleanOutput, "\n");
				fprintf(cleanOutput, "%s_%sBREAK_HERE_PALINDROME", basename(argv[1]), temporary_input_string);
				current_size += 1;
			} else {
				fprintf(cleanOutput, "%s", temporary_input_string);
			}
		} else{
			free(output_filename);
			free(temporary_input_string);
			fprintf(stderr, "%s%d\n", "Lines too long. Current line limit: ", BUFFER_LINES_IN);
			return -1;
		}
	}

	free(output_filename);
	free(temporary_input_string);
	fclose(cleanOutput);
	fclose(list_of_files);
	return 0;


}

