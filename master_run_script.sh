echo -e "\n
==========================    PalindromeFinder    ==========================
==========================        iDas Lab        ==========================
==========================         v. 0.1         ==========================
\n\nPlease note: This script only supports one FASTA file at a time
\n\nPlease wait while the scripts are compiled and prepared.\n"

(cd cleanUpData && make all)
(cd intermediate_data && find . -name "*" -print0 | xargs -0 rm)
rm -rf intermediate_data
mkdir intermediate_data
(cd cleanUpData && sbt package)
(cd PalindromeFinder && sbt package)

if test "$#" -eq 0; then
echo -e "\nPlease give the path to the file you would like to process:"
read filepath
(cd cleanUpData && ./cleanUpFirst $filepath)
	if test "$?" -ne 0; then
		echo -e "Error encountered."
		exit
	fi
echo -e "What is the minimum length palindrome you would like to identify?  \c"
read minimum
echo -e "Please wait while the file is parsed and prepared...\c"
(cd cleanUpData && /share/sw/spark/spark-1.4.1-hadoop2.6/bin/spark-submit  target/scala-2.10/cleanupdata_2.10-0.1.jar ../intermediate_data/cleanOutput_stage1.txt $minimum)
rm intermediate_data/cleanOutput_stage1.txt
echo -e "\n"
echo -e "Please enter the following Spark configuration parameters: \nMaster Node? \c"
read master 
echo -e "Number of executors? \c"
read executor_num
echo -e "Memory per executor? \c"
read executor_mem
echo -e "Number of cores per executor? \c"
read executor_cores
echo -e "Master memory? \c"
read master_mem

#hadoop fs -rm -r intermediate_data
#echo -e "Transferring Files to HDFS. Please wait...\c"
#hadoop fs -put intermediate_data

echo -e "\n\nTransfer complete. Starting to process. Please wait...\c"

for f in `ls intermediate_data`
do
	(cd PalindromeFinder && /share/sw/spark/spark-1.4.1-hadoop2.6/bin/spark-submit --master $master --driver-memory $master_mem --num-executors $executor_num --executor-cores $executor_cores --executor-memory $executor_mem --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/../intermediate_data/$f $minimum)
done

(cd intermediate_data && find . -name "*" -print0 | xargs -0 rm)
exit

fi

if test "$#" -eq 7; then

filepath=$1
minimum=$2
master=$3
executor_num=$4
executor_mem=$5
executor_cores=$6
master_mem=$7

(cd cleanUpData && ./cleanUpFirst $filepath)
	if test "$?" -ne 0; then
		echo -e "Error encountered."
		exit
	fi
(cd cleanUpData && /share/sw/spark/spark-1.4.1-hadoop2.6/bin/spark-submit  target/scala-2.10/cleanupdata_2.10-0.1.jar ../intermediate_data/cleanOutput_stage1.txt $minimum)
rm intermediate_data/cleanOutput_stage1.txt

for f in `ls intermediate_data`
do
	(cd PalindromeFinder && /share/sw/spark/spark-1.4.1-hadoop2.6/bin/spark-submit --master $master --driver-memory $master_mem --num-executors $executor_num --executor-cores $executor_cores --executor-memory $executor_mem --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/../intermediate_data/$f $minimum)
done

(cd intermediate_data && find . -name "*" -print0 | xargs -0 rm)

exit
fi

echo -e "usage: master_run_script.sh <filepath> <minimum> <master> <executor_num> <executor_mem> <executor_cores> <master_mem>"