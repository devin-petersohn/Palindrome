# Palindrome
###A Palindrome Finder using Apache Spark

Requirements for use:

* Apache Spark 1.3
* Scala 2.10.4
* Java 1.6
* SBT 0.13.8

A build file is included in this repository for compiling the scala script into a .jar file. To compile the code, please use the following command when you are in the Palindrome directory:

`sbt package`

Once you have compiled the source code, you can run a spark job using the `spark-submit` command with the parameters required for your spark setup. The program requires that your FASTA chromosome file be passed as the only argument.

The current implementation is specific to our setup and pipeline. The `.txt1` files are not shifted at all and the `.txt4` files are shifted by 189. This will be changed in future implementations.

The output is written to the current users HDFS under the directory `results/palindromes`.

For more information, email Devin Petersohn at <dpvx8@mail.missouri.edu>