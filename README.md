# Palindrome
###A Palindrome Finder using Apache Spark

Requirements for use:

* Apache Spark 1.3
* Scala 2.10.4
* Java 1.6
* SBT 0.13.8

This repository includes a full pipeline from cleaning the input sequence to outputting the palindromes to HDFS. 

######Preparing the data

To prepare the data, you will need to run the `cleanUp.c` script to properly parse the input data. The data doesn’t have to be very clean for this parser. You may pass in multiple fasta sequences delineated by `>IDENTIFIER` . The `IDENTIFIER` should be unique and meaningful to you, as this is how the results will stored for searching later.

Once you have run the `cleanUp.c` script, you will need to run the `cleanUpData.scala` script. This script makes sure that the position count will be accurate during the processing. It outputs separate files for each fasta sequence. Currently the files are stored in the working directory, but that will change.

######Running the Palindrome Finder

`usage: spark-submit \`  
`<spark parameters> <palindromefinder jar> \`  
`<input filename> <minimum palindrome length>`

* The `<spark parameters>` are typically specific to your system.
* The `<palindromefinder jar>` is the jar that is output from `sbt`.
* The `<input filename>` is a single filename for palindrome extraction. It must have been formatted by the other scripts in order to be guaranteed to run correctly.
* The `<minimum palindrome length>` is the smallest size of palindrome you would like to extract.

The output is written to the current users HDFS under the directory `results/palindromes/<input_filename>`.

######Compiling the Source

Two build files are included in this repository for compiling the scala script into a .jar file. To compile the code, please use the following command:

`sbt package`

You must be in the correct directory to run this command. 

######Future Work

We will get a shell script that can automate this process, to include building and compiling the source.

For more information, email Devin Petersohn at <devin.petersohn@gmail.com>