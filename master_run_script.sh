echo -e "

==========================    PalindromeFinder    ==========================
==========================        iDas Lab        ==========================
==========================         v. 0.1         ==========================


Please note: This script only supports one FASTA file at a time


Please wait while the scripts are compiled and prepared.
"

(cd cleanUpData && make all)
rm -rf intermediate_data
mkdir intermediate_data
(cd cleanUpData && sbt package)
(cd PalindromeFinder && sbt package)

if test "$#" -eq 0; then
echo -e "
Please give the path to the file you would like to process:"
read filepath
(cd cleanUpData && ./cleanUpFirst $filepath)
	if test "$?" -ne 0; then
		echo -e "Error encountered."
		exit
	fi
echo -e "What is the minimum length palindrome you would like to identify?  \c"
read minimum
echo -e "Please wait while the file is parsed and prepared...\c"
(cd cleanUpData && spark-submit target/scala-2.10/cleanupdata_2.10-0.1.jar ../intermediate_data/cleanOutput_stage1.txt $minimum)
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

echo -e "\nThank you. Starting to process. Please wait...\c"
echo -e "\nDeleting intermediate_data directory on HDFS. Continue? Y/N:	\c"
read continue_choice
if [ "$continue_choice" = "N" ] || [ "$continue_choice" = "n" ]; then
	exit
fi
while [ "$continue_choice" = "Y" ] || [ "$continue_choice" = "y" ]
do
	echo -e "Invalid input. "
	echo -e "Continue? Y/N:	\c"
	read continue_choice
done

hadoop fs -rm -rf intermediate_data
hadoop fs -put intermediate_data

for f in `hadoop fs -ls intermediate_data`
do
	(cd PalindromeFinder && spark-submit --master $master --driver-memory $master_mem --num-executors $executor_num --executor-cores $executor_cores --executor-memory $executor_mem --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/../intermediate_data/$f $minimum)
done

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
(cd cleanUpData && spark-submit target/scala-2.10/cleanupdata_2.10-0.1.jar ../intermediate_data/cleanOutput_stage1.txt $minimum)
rm intermediate_data/cleanOutput_stage1.txt

for f in `ls intermediate_data`
do
	(cd PalindromeFinder && spark-submit --master $master --driver-memory $master_mem --num-executors $executor_num --executor-cores $executor_cores --executor-memory $executor_mem --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/../intermediate_data/$f $minimum)
done


exit
fi

echo -e "usage: master_run_script.sh <filepath> <minimum> <master> <executor_num> <executor_mem> <executor_cores> <master_mem>"