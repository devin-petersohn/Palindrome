echo -e "\n
==========================    PalindromeFinder    ==========================
==========================        iDas Lab        ==========================
==========================         v. 0.1         ==========================
\n\nPlease note: This script only supports one FASTA file at a time
\n\nPlease wait while the scripts are compiled and prepared.\n"

module load sbt
module load mri/hdfs
(cd cleanUpData && make all)
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

(cd PalindromeFinder && /share/sw/spark/spark-1.4.1-hadoop2.6/bin/spark-submit --driver-memory 10G --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/../intermediate_data/`basename $filepath`.clean $minimum)

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

(cd PalindromeFinder && spark-submit --master $master --driver-memory $master_mem --num-executors $executor_num --executor-cores $executor_cores --executor-memory $executor_mem --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/../intermediate_data/`basename $filepath`.clean $minimum)

exit
fi

echo -e "usage: master_run_script.sh <filepath> <minimum> <master> <executor_num> <executor_mem> <executor_cores> <master_mem>"
