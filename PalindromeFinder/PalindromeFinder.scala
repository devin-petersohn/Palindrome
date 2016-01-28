import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._
import java.io._
import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.log4j.PropertyConfigurator

object PalindromeFinder {
	/*
	*
	* Method: filterSeqs
	* Purpose: Filters out strings that contain invalid characters to reduce dataset
	* Parameters:
	* 	string: String
	*		The string to be verfied
	*	initWindowSize: Int
	*		The initial size of the sliding window
	* Return: Boolean
	*	Valid string returns true
	*	Invalid string returns false
	*
	*/
def filterSeqs(string: String, initWindowSize: Int): Boolean = {
//if(string.contains("N")) return false
for(i <- string) {
if(i != 'A' && i != 'C' && i != 'T' && i != 'G') return false
}
return true
}

	/*
	*
	* Method: positiveAndNegative
	* Purpose: Ensures that there is a positive and negative value in the list.
	*	This is how we verify that the sequence exists on both strands.
	* Parameters:
	*	list: Iterable[Int]
	*		The list of positions that will be verified.
	* Return: Boolean
	*	Valid list of positions returns true
	*	Invalid list of positions returns false
	*
	*/
	def positiveAndNegative(list: Iterable[Int]): Boolean = {
		var neg = false
		var pos = false
		for(i <- list) {
			if(neg == true && pos == true) return true
			else {
				if(i < 0) neg = true
				else pos = true
			}
		}
		if(neg == true && pos == true) return true
		else return false
	}

	/*
	*
	* Method: verifyPalindromic
	* Purpose: Checks the candidate palindromes against each other, verifying that the sequence is palindromic.
	*	This is where we check for overlap in the main algorithm. Finds palindromes from input length to 2 * input length.
	* Parameters: 
	*	list: Iterable[Int]
	*		The list of positions for a candidate. Some of these positions may be valid while others may not be.
	*	length: Int
	*		The length of the current k-block
	* Return: Iterable[(Int, Int)]
	*	Returns a collection of tuples that represent valid overlapping pairs.
	* 		The first value in each tuple is the starting position of a verified palindrome
	*		The second value in each tuple is the length of the verified palindrome
	*			This value is translated back to the input strand position before being added to the collection
	*		If no values exist, returns an empty collection
	*
	*/
	def verifyPalindromic(list: Iterable[Int], length: Int): Iterable[((Int,Int))] = {
		val negVals = list.filter(_ < 0)
		val posVals = list.filter(_ > 0)
		var finalList = ArrayBuffer[((Int,Int))]()
		for(i <- posVals) {
			for(j <- negVals) {
				if((j * -1 - length) < (i + length) && (j * -1) > i) {
					finalList += ((i,Math.abs((j * -1) - i)))
				}
			}
		}
		return finalList
	}

	/*
	*
	* Method: possiblePalindrome
	* Purpose: Helper function for filterNonPalindromic, checks that each pair is complemetary
	* Parameters:
	*	string: String
	*		The input string to be verified
	* Return: Boolean
	*	Valid input string returns true
	*	Invalid input string returns false
	*
	*/
	def possiblePalindrome(string: String): Boolean = {
		if( string.contains("AT") || string.contains("TA") || string.contains("CG") || string.contains("GC")) return true
		else return false
	}

	/*
	*
	* Method: filterNonPalindromic
	* Purpose: Fine-grained function to filter out those sequences that could not possibly be palindromic
	*	Checks that there is no pair around which each pair branching out from the center is complementary
	* Parameters:
	*	string: String
	*		The input string to be verified
	* Return: Boolean
	*	Valid input string returns true
	*	Invalid input string returns false
	*
	*/
	def filterNonPalindromic(string: String): Boolean = {
		var mid = string.length/2
		val shift = 1
		var counter = 1
		var isPal = false
		var lastValue = 0
		while(mid != string.length) {
			counter = 1
			breakable {
				for(i <- mid until string.length) {
					val left = string.charAt(i - counter)
					isPal = possiblePalindrome(left.toString + string.charAt(i).toString)
					lastValue = i
					counter+=2
					if(isPal == false) break
				}
			}
			if(isPal == true && lastValue == string.length-1) return true
			mid+=shift
		}
		return false
	}

	/*
	*
	* Method: extendPalindromicSequence
	* Purpose: Extends the full palindromic sequence based on the overlap
	* Parameters:
	*	palindrome: ((String, String), Array[((Int, Int))])
	*		The palindrome to be extended.
	*			candidates._1._1: the sequence to be extended
	*			candidates._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*			candidates._2: an array of starting positions and lengths for each instance of a palindrome from the given sequence
	* Return: Array[((String, String), Int)]
	*	Returns a collection of tuples that consists of all fully extended palindromes from the input sequence in the origin
	*		returned._1._1: the full palindrome after extension
	*		returned._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*		returned._2: the starting position of this specific palindrome
	*
	*/
	def extendPalindromicSequence(palindrome: ((String,String), Array[((Int,Int))])): Array[((String,String), Int)] = {
		val positions = palindrome._2
		val sequence = palindrome._1._1
		val species = palindrome._1._2
		var finalList = ArrayBuffer[((String,String), Int)]()

		for(i <- positions) {
			finalList += ((((sequence + complement(sequence.dropRight(sequence.length - (i._2 - sequence.length)), species)._1, species)), i._1))
		}
		return finalList.toArray
	}

	/*
	*
	* Method: extractPalindromes
	* Purpose: Palindrome extraction phase of the algorithm.  Checks for overlap to identify palilndrome
	* Parameters:
	*	candidates: org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])]
	*		An RDD of candidate sequences that have passed the fine grained filter.
	*			candidates[]._1._1: the sequence to be extended
	*			candidates[]._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*			candidates[]._2: a collection of starting positions for each instance of a given sequence
	*	currentLength: Int
	*		The length of the current k-block
	* Return: org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])]
	*	An RDD of all palindromes between the currentLength and 2 * currentLength
	*		returned[]._1._1: a palindromic sequence
	*		returned[]._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*		returned[]._2: a collection of starting positions for each instance of a given palindrome
	*
	*/
	def extractPalindromes(candidates: org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])], currentLength: Int): org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])] = {
		return (candidates
		.map(f => ((f._1, ((f._2.filter(_ > 0), f._2.filter(_ < 0))))))
		.filter(t => t._2._1.size > 0 && t._2._2.size > 0)
		.filter(candidate => filterNonPalindromic(candidate._1._1))
		.map(g=> ((g._1, ((g._2._1.flatMap(f => 
				List(((f+currentLength - 1)/currentLength, f),((f)/currentLength, f))) ++ g._2._2.flatMap(f => List(((Math.abs(((f * -1) - currentLength)/currentLength), f)),((Math.abs(((f * -1) - 1)/currentLength), f)))))
				.groupBy(_._1).map(f => f._2.map(x => x._2))
				.filter(f => positiveAndNegative(f)).map(z => verifyPalindromic(z, currentLength)).flatten).toSet.toArray)))
		.filter(h => h._2.size > 0)
		.flatMap(pal => extendPalindromicSequence(pal))
		.groupByKey)
	}

	/*
	*
	* Method: complement
	* Purpose: Calculates the reverse complement of the string
	* Parameters:
	* 	sequence: ((String, String))
	*		A tuple that represents a given string.
	*			sequence._1: the sequence to have the reverse compelement calculated
	*			sequence._2: the identification of the origin of this specific sequence (chromosome id and species id)
	* Return: ((String, String))
	*	Returns a tuple that contains the reverse complement of the input sequence.
	*		returned._1: the reverse compelement of the input sequence
	*		returned._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*
	*/
	def complement(sequence: ((String,String))): ((String,String)) = {
		return (((sequence._1.replace('A','*').replace('T','A').replace('*','T').replace('C','*').replace('G','C').replace('*','G')).reverse, sequence._2))
	}

	/*
	* 
	* Method: merge
	* Purpose: Merges the strings of adjacent blocks together as a part of the doubling phase of the algorithm
	* Parameters:
	*	x: ((String, String))
	*		The first input string. Whether this string occurs first in the original sequence is unkown.
	*			x._1: the input sequence
	*			x._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*	y: ((String, String))
	*		The second input string. Whether this string occurs first in the original sequence is unkown.
	*			y._1: the input sequence
	*			y._2: the identification of the origin of this specific sequence (chromosome id and species id)
	* Return: ((String, String))
	*	Returns a tuple that contains the merged string that has been doubled.
	*		returned._1: the merged string
	*		returned._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*
	*/
	def merge(x: String, y: String): String = { 
		if(x.contains("*")) return x.dropRight(1)+y
		else return y.dropRight(1)+x
	}

	/*
	*
	* Method: coarseGrainedAggregation
	* Purpose: Merges adjacent building blocks, effectively doubling the size of the blocks and removing non-repeats
	* Parameters:
	*	blocks: org.apache.spark.rdd.RDD[((String, String), Int)]
	*		An RDD of k-blocks.
	*			blocks[]._1._1: the sequence of the current k-block
	*			blocks[]._1._2: the indentification of the origin of this specific sequence
	*			blocks[]._2: the position of this individual k-block
	*	windowSize: Int
	*		The size of the current k-block.
	* Return: org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])]
	*	An RDD of doubled sequences to be verified. 
	*		returned[]._1._1: the sequence of the doubled k-block
	*		returned[]._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*		returned[]._2: a collection of all position of this specific sequence
	*
	*/
	def coarseGrainedAggregation(blocks: org.apache.spark.rdd.RDD[((String, String), Int)], windowSize: Int): org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])] = {
		return blocks.map(_.swap)
		.flatMap(f => Iterable(((f._1, f._2._2),f._2._1),(((f._1 + f._2._1.length), f._2._2),((f._2._1+"*")))))
		.reduceByKey((a,b) => merge(a,b)).map(z => ((z._1._1, ((z._2, z._1._2)))))
		.filter(_._2._1.length>windowSize+1)
		.map(f => (f._2,((f._1-f._2._1.length/2))))
		.groupByKey
		.filter(_._2.size>1)
	}

	/*
	*
	* Method: applyPositionToSequence
	* Purpose: Fans out the tuples such that every tuple has exactly one position. The collection of positions is applied to the sequence.
	* Parameters:
	*	groupedPos: org.apache.spark.rdd.RDD[((String,String), Iterable[Int])]
	*		An RDD of tuples which contains all positions of each sequence grouped together in a collection
	*			groupedPos[]._1._1: the sequence
	*			groupedPos[]._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*			groupedPos[]._2: the collection of positions that contains all occurrences of this sequence
	* Return: org.apache.spark.rdd.RDD[((String, String), Int)]
	*	An RDD of tuples that contains all positions, however they are no longer grouped
	*		returned[]._1._1: the sequence
	*		returned[]._1._2: the identification of the origin of this specific sequence (chromosome id and species id)
	*		returned[]._2: a position of an instance of this sequence
	*
	*/
	def applyPositionToSequence(groupedPos: org.apache.spark.rdd.RDD[((String,String), Iterable[Int])]) : org.apache.spark.rdd.RDD[((String, String), Int)] = {
		return groupedPos.flatMap(f => f._2.map(g => ((f._1, g))))
	}

	/*
	*
	* Method: main
	* Purpose: Extract all palindromic sequences starting from an initial length up to an arbitrary size
	* Parameters: 
	*	args: Array[String]
	*		The arguments to the main program are as follows:
	*			args[0]: The filename of the FASTA file. All palindromes will be extracted from this sequence
	*			args[1]: The minimum length of palindrome to be extracted.
	* Return: NOT USED
	*
	*/
	def main(args: Array[String]) = {
		
		val sc = new SparkContext()
		val initWindowSize = args(1).toInt
		val file = sc.textFile(args(0), 80)

		//
		val words = file.flatMap(line => line.split("BREAK_HERE_PALINDROME")(1).sliding(initWindowSize).zipWithIndex.filter(seq => filterSeqs(seq._1, initWindowSize)).map(k_block => ((k_block._1, line.split("BREAK_HERE_PALINDROME")(0).replaceAll("[>. /]", "_")), k_block._2+1)))
		//		val words = file.zipWithIndex.flatMap( l => ( l._1.sliding(initWindowSize).zipWithIndex.filter(seq => filterSeqs(seq._1, initWindowSize)).map( f => ((( f._1, chrID)),((f._2+1)+((line_length)*(l._2 - 1))).toInt))))

		val compWords = words.map(f => ((complement(f._1), -1 * (f._2 + f._1._1.length))))

		//all the words of length equal to initWindowSize
		val allWords = sc.union(words,compWords).groupByKey
		//set this variable for the start of the loop
		var palindromes = allWords
		//set this variable for the start of the loop
		var repeated_sequences = allWords
		//iteration will double for every time the loop is completed
		var iteration = 1

		breakable {
			while(true){			
				if(iteration > 1) repeated_sequences = coarseGrainedAggregation(applyPositionToSequence(repeated_sequences), initWindowSize * iteration / 2)

				palindromes = extractPalindromes(repeated_sequences, initWindowSize * iteration)
				if(!palindromes.isEmpty) palindromes.saveAsObjectFile("/idas/results/palindromes/" + initWindowSize * iteration + "/" + args(0).split("/").last.split(".clean")(0))

				else break
				iteration *= 2
			}
		}


//Below is the old method.
/*
		val smallestPalindromes = extractPalindromes(allWords, initWindowSize)
		if(!smallestPalindromes.isEmpty) {
			smallestPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize + "/" + chrID)

			val doubleLengthWords = coarseGrainedAggregation(applyPositionToSequence(allWords), initWindowSize)
			val doublePalindromes = extractPalindromes(doubleLengthWords, initWindowSize * 2)	
			if(!doublePalindromes.isEmpty) {
				doublePalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 2 + "/" + chrID)

				val fourTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(doubleLengthWords), initWindowSize * 2)
				val fourTimesPalindromes = extractPalindromes(fourTimesLengthWords, initWindowSize * 4)		
				if(!fourTimesPalindromes.isEmpty) {
					fourTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 4 + "/" + chrID)

					val eightTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(fourTimesLengthWords), initWindowSize * 4)
					val eightTimesPalindromes = extractPalindromes(eightTimesLengthWords, initWindowSize * 8)
					if(!eightTimesPalindromes.isEmpty) {
						eightTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 8 + "/" + chrID)

						val sixteenTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(eightTimesLengthWords), initWindowSize * 8)
						val sixteenTimesPalindromes = extractPalindromes(sixteenTimesLengthWords, initWindowSize * 16)
						if(!sixteenTimesPalindromes.isEmpty) {
							sixteenTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 16 + "/" + chrID)

							val thirtytwoTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(sixteenTimesLengthWords), initWindowSize * 16)
							val thirtytwoTimesPalindromes = extractPalindromes(thirtytwoTimesLengthWords, initWindowSize * 32)
							if(!thirtytwoTimesPalindromes.isEmpty) {
								thirtytwoTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 32 + "/" + chrID)

								val sixtyfourTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(thirtytwoTimesLengthWords), initWindowSize * 32)
								val sixtyfourTimesPalindromes = extractPalindromes(sixtyfourTimesLengthWords, initWindowSize * 64)
								if(!sixtyfourTimesPalindromes.isEmpty) {
									sixtyfourTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 64 + "/" + chrID)

									val onetwentyeightTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(sixtyfourTimesLengthWords), initWindowSize * 64)
									val onetwentyeightTimesPalindromes = extractPalindromes(onetwentyeightTimesLengthWords, initWindowSize * 128)
									if(!onetwentyeightTimesPalindromes.isEmpty) {
										onetwentyeightTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 128 + "/" + chrID)

										val twofiftysixTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(onetwentyeightTimesLengthWords), initWindowSize * 128)
										val twofiftysixTimesPalindromes = extractPalindromes(twofiftysixTimesLengthWords, initWindowSize * 256)
										if(!twofiftysixTimesPalindromes.isEmpty) {
											twofiftysixTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 256 + "/" + chrID)

											val fivetwelveTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(twofiftysixTimesLengthWords), initWindowSize * 256)
											val fivetwelveTimesPalindromes = extractPalindromes(fivetwelveTimesLengthWords, initWindowSize * 512)
											if(!fivetwelveTimesPalindromes.isEmpty) { 
												fivetwelveTimesPalindromes.saveAsObjectFile("results/other_researcher_data/palindromes/" + initWindowSize * 512 + "/" + chrID)
											
											
											}//512TimePal

										}//256TimePal

									}//128TimePal

								}//sixtyfourTimePal

							}//thirtytwoTimePal

						}//sixteenTimePal

					}//eightTimePal

				}//fourTimePal

			}//doublePal

		}//smallestPal
*/
	}

}
