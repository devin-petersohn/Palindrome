echo -e "\n
==========================    PalindromeFinder    ==========================
==========================        iDas Lab        ==========================
==========================         v. 0.1         ==========================
\n\nPlease note: This script only supports one FASTA file at a time
\n\nPlease wait while the scripts are compiled and prepared.\n"

module load sbt
module load mri/hdfs
module load spark
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

for f in `cat intermediate_data/list_of_files.clean`; do

(cd PalindromeFinder && srun -p GPU --mem=10G spark-submit --driver-memory 10G --deploy-mode cluster --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/$f $minimum)

done
exit

fi

if test "$#" -eq 2; then

filepath=$1
minimum=$2

(cd cleanUpData && ./cleanUpFirst $filepath)
	if test "$?" -ne 0; then
		echo -e "Error encountered."
		exit
	fi
for f in `cat intermediate_data/list_of_files.clean`; do

(cd PalindromeFinder && srun -p GPU --mem=10G spark-submit --driver-memory 10G --deploy-mode cluster --class PalindromeFinder target/scala-2.10/palindromefinder_2.10-0.1.jar file://`pwd`/$f $minimum)

done

exit
fi

echo -e "usage: master_run_script.sh <filepath> <minimum>"
