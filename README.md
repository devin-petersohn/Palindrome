# Palindrome
###A Palindrome Finder using Apache Spark

Requirements for use:

* Apache Spark 1.3
* Scala 2.10.4
* Java 1.6
* SBT 0.13.8

This repository includes a full pipeline from cleaning the input sequence to outputting the palindromes to HDFS. 

####Using the included `master_run_script.sh`
######Single job mode

This mode will give you prompts for every input and walk you through every step to ensure the output is correct. Simply run the script: 

	bash master_run_script.sh 

If you do not include any arguments, you will run this script in Single job mode.

######Batch job mode
This mode will allow you to pass in all the parameters at a single time to allow for large scale batch processing. Run the script as follows:

	usage: bash master_run_script.sh \
	<filepath> <minimum> <master> \
	<executor_num> <executor_mem> \  
	<executor_cores> <master_mem> 

If you do not include enough parameters, the script will not allow you to continue. You must run in either Single or Batch job mode.

####Manually Building and Running the Project
######Preparing the data

To prepare the data, you will need to run the `cleanUp.c` script to properly parse the input data. The data doesn’t have to be very clean for this parser. You may pass in multiple fasta sequences delineated by `>IDENTIFIER` . The `IDENTIFIER` should be unique and meaningful to you, as this is how the results will stored for searching later.

* Step 1: Compile and run `cleanUp.c`. There is an included Makefile for convenience.
	
		usage: cleanUpFirst <fasta_file>

* Step 2: Compile and run `cleanUpData.scala`. There is an included SBT build file for convenience.

		usage: spark-submit \
		target/scala-2.10/cleanupdata_2.10-0.1.jar \
		../intermediate_data/cleanOutput_stage1.txt \
		<minimum_palindrome_length>

######Running the Palindrome Finder

First, you must build the Spark project using the included SBT build file. Once the project has been built, you may run with the following syntax:

	usage: spark-submit \
	<spark parameters> <palindromefinder jar> \
	<input filename> <minimum palindrome length>

* The `<spark parameters>` are typically specific to your system.
* The `<palindromefinder jar>` is the jar that is output from `sbt`.
* The `<input filename>` is a single filename for palindrome extraction. It must have been formatted by the other scripts in order to be guaranteed to run correctly.
* The `<minimum palindrome length>` is the smallest size of palindrome you would like to extract.

The output is written to the current users HDFS under the directory `results/palindromes/<input_filename>`.

######Compiling the Source

Two build files are included in this repository for compiling the scala script into a .jar file. To compile the code, please use the following command:

	sbt package

You must be in the correct directory to run this command. 


For more information, email Devin Petersohn at <devin.petersohn@gmail.com>