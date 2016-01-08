echo "

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
echo "
Please give the path to the file you would like to process.
"
read filepath
(cd cleanUpData && ./cleanUpFirst $filepath)
	if test "$?" -ne 0; then
		echo "Error encountered.
"
		exit
	fi
echo "What is the minimum length palindrome you would like to identify?  "
read minimum
echo "Please wait while the file is parsed..."
(cd cleanUpData && spark-submit target/scala-2.10/cleanupdata_2.10-0.1.jar ../intermediate_data/cleanOutput_stage1.txt $minimum)
rm intermediate_data/cleanOutput_stage1.txt
echo "Please enter the following Spark configuration parameters: 
Master Node? "
read master 
echo "Number of executors? "
read executor_num
echo "Memory per executor? "
read executor_mem
echo "Number of cores per executor? "
read executor_cores
echo "Master memory? "
read master_mem

echo "
Thank you. Starting to process. Please wait...

"

for f in `ls intermediate_data`
do
	(cd PalindromeFinder && spark-submit --master $master --driver-memory $master_mem --num-executors $executor_num --executor-cores executor-cores --executor-memory executor_mem --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar ../intermediate_data/$f $minimum)
done

exit

fi