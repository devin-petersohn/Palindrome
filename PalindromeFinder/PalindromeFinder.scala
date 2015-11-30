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
	//
	//filter out these strings to reduce dataset
	//
	def filterSeqs(string: String, initWindowSize: Int): Boolean = {
		if(string.contains("N")) return false
		for(i <- string) {
			if(i != 'A' && i != 'C' && i != 'T' && i != 'G') return false
		}
		return true
	}

	//
	//ensures that there is a positive and negative value in the list
	//
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

	//
	//function to verify palindromic, checks the candidates against each other
	//
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

	//
	//helper function for filterNonPalindromic
	//
	def possiblePalindrome(string: String): Boolean = {
		if( string.contains("AT") || string.contains("TA") || string.contains("CG") || string.contains("GC")) return true
		else return false
	}

	//
	//fine-grained function to filter out those sequences that could not possibly be palindromic
	//
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

	//
	//gets the full palindromic sequence based on the overlap
	//
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

	//
	//Palindrome extraction phase of the algorithm.  Checks for overlap to identify palilndrome
	//
	def extractPalindromes(candidates: org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])], currentLength: Int): org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])] = {
		return (candidates
		.map(f => ((f._1, ((f._2.filter(_ > 0), f._2.filter(_ < 0)))))).filter(t => t._2._1.size > 0 && t._2._2.size > 0)
		.filter(candidate => filterNonPalindromic(candidate._1._1))
		.map(g=> ((g._1, ((g._2._1.flatMap(f => List(((f+currentLength - 1)/currentLength, f),((f)/currentLength, f))) ++ g._2._2.flatMap(f => List(((Math.abs(((f * -1) - currentLength)/currentLength), f)),((Math.abs(((f * -1) - 1)/currentLength), f)))))
		.groupBy(_._1).map(f => f._2.map(x => x._2))
		.filter(f => positiveAndNegative(f)).map(z => verifyPalindromic(z, currentLength)).flatten).toSet.toArray))).filter(h => h._2.size > 0).flatMap(pal => extendPalindromicSequence(pal)).groupByKey)
	}

	//
	//calculates the reverse complement of the string
	//
	def complement(sequence: ((String,String))): ((String,String)) = {
		return (((sequence._1.replace('A','*').replace('T','A').replace('*','T').replace('C','*').replace('G','C').replace('*','G')).reverse, sequence._2))
	}

	//function to merge adjacent blocks together
	def merge(x: ((String,String)), y: ((String,String))): ((String,String)) = { 
		if(x._1.contains("*")) return ((x._1.dropRight(1)+y._1 , x._2))
		else return ((y._1.dropRight(1)+x._1, x._2))
	}

	//
	//function to merge adjacent building blocks, effectively doubling the size of the blocks and removing non-repeats
	//
	def coarseGrainedAggregation(blocks: org.apache.spark.rdd.RDD[((String, String), Int)], windowSize: Int): org.apache.spark.rdd.RDD[((String, String), Iterable[(Int)])] = {
		return blocks.map(_.swap)
		.flatMap(f => Iterable((f._1,f._2),((f._1 + f._2._1.length),((f._2._1+"*",f._2._2)))))
		.reduceByKey((a,b) => merge(a,b))
		.filter(_._2._1.length>windowSize+1)
		.map(f => (f._2,((f._1-f._2._1.length/2))))
		.groupByKey
		.filter(_._2.size>1)
	}

	//
	//Fans out the tuples such that every tuple has exactly one position
	//
	def applyPositionToSequence(groupedPos: org.apache.spark.rdd.RDD[((String,String), Iterable[Int])]) :org.apache.spark.rdd.RDD[((String, String), Int)] = {
		return groupedPos.flatMap(f => f._2.map(g => ((f._1, g))))
	}


	def main(args: Array[String]) = {
		
		val sc = new SparkContext()
		val initWindowSize = args(1).toInt
		val file = sc.textFile(args(0), 80)
		val chrID = args(0) //the filename is also the chromosome id, for a unique storage name
		//200 is the length of our lines output from the pipeline
		val line_length = 200 //this is hardcoded to avoid a miscount on the last line of the file

		val words = file.zipWithIndex.flatMap( l => ( l._1.sliding(initWindowSize).zipWithIndex.filter(seq => filterSeqs(seq._1, initWindowSize)).map( f => ((( f._1, chrID)),((f._2+1)+(line_length*(l._2))).toInt))))
		val compWords = words.map(f => ((complement(f._1), -1 * (f._2 + f._1._1.length))))

		//all the words of length equal to initWindowSize
		val allWords = sc.union(words,compWords).groupByKey
		var palindromes = allWords
		var iteration = 1
		
		breakable{
			while(true){			
				if(iteration > 1) palindromes = coarseGrainedAggregation(applyPositionToSequence(palindromes), initWindowSize * iteration)
				iteration *= 2
				palindromes = extractPalindromes(palindromes, initWindowSize * iteration)
				if(!palindromes.isEmpty) palindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * iteration + "/" + chrID)
				else break
			}
		}


//Below is the old method.
/*
		val smallestPalindromes = extractPalindromes(allWords, initWindowSize)
		if(!smallestPalindromes.isEmpty) {
			smallestPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize + "/" + chrID)

			val doubleLengthWords = coarseGrainedAggregation(applyPositionToSequence(allWords), initWindowSize)
			val doublePalindromes = extractPalindromes(doubleLengthWords, initWindowSize * 2)	
			if(!doublePalindromes.isEmpty) {
				doublePalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 2 + "/" + chrID)

				val fourTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(doubleLengthWords), initWindowSize * 2)
				val fourTimesPalindromes = extractPalindromes(fourTimesLengthWords, initWindowSize * 4)		
				if(!fourTimesPalindromes.isEmpty) {
					fourTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 4 + "/" + chrID)

					val eightTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(fourTimesLengthWords), initWindowSize * 4)
					val eightTimesPalindromes = extractPalindromes(eightTimesLengthWords, initWindowSize * 8)
					if(!eightTimesPalindromes.isEmpty) {
						eightTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 8 + "/" + chrID)

						val sixteenTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(eightTimesLengthWords), initWindowSize * 8)
						val sixteenTimesPalindromes = extractPalindromes(sixteenTimesLengthWords, initWindowSize * 16)
						if(!sixteenTimesPalindromes.isEmpty) {
							sixteenTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 16 + "/" + chrID)

							val thirtytwoTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(sixteenTimesLengthWords), initWindowSize * 16)
							val thirtytwoTimesPalindromes = extractPalindromes(thirtytwoTimesLengthWords, initWindowSize * 32)
							if(!thirtytwoTimesPalindromes.isEmpty) {
								thirtytwoTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 32 + "/" + chrID)

								val sixtyfourTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(thirtytwoTimesLengthWords), initWindowSize * 32)
								val sixtyfourTimesPalindromes = extractPalindromes(sixtyfourTimesLengthWords, initWindowSize * 64)
								if(!sixtyfourTimesPalindromes.isEmpty) {
									sixtyfourTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 64 + "/" + chrID)

									val onetwentyeightTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(sixtyfourTimesLengthWords), initWindowSize * 64)
									val onetwentyeightTimesPalindromes = extractPalindromes(onetwentyeightTimesLengthWords, initWindowSize * 128)
									if(!onetwentyeightTimesPalindromes.isEmpty) {
										onetwentyeightTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 128 + "/" + chrID)

										val twofiftysixTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(onetwentyeightTimesLengthWords), initWindowSize * 128)
										val twofiftysixTimesPalindromes = extractPalindromes(twofiftysixTimesLengthWords, initWindowSize * 256)
										if(!twofiftysixTimesPalindromes.isEmpty) {
											twofiftysixTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 256 + "/" + chrID)

											val fivetwelveTimesLengthWords = coarseGrainedAggregation(applyPositionToSequence(twofiftysixTimesLengthWords), initWindowSize * 256)
											val fivetwelveTimesPalindromes = extractPalindromes(fivetwelveTimesLengthWords, initWindowSize * 512)
											if(!fivetwelveTimesPalindromes.isEmpty) { 
												fivetwelveTimesPalindromes.saveAsObjectFile("results/palindromes/" + initWindowSize * 512 + "/" + chrID)
											
											
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
