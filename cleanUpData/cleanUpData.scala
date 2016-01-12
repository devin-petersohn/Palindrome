import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}
import java.io._

object Cleanup {
	def main(args: Array[String]) = {

		val filename = args(0)
		//var pw = new PrintWriter(new File("../intermediate_data/temporary_fasta_file.txt"))
		var path = "../intermediate_data/"
		var prevline = ""
		var fasta_id = ""
		var lineNum = 1
		var full_sequence = ""

		try {
			for (line <- Source.fromFile(filename).getLines()) {
				if(line.charAt(0) != '>'){
					for(slide <- line.sliding(200,200)){
						if(!prevline.equals("")) {
							full_sequence += prevline + slide.dropRight(slide.length - args(1).toInt + 1) + "\n"
						//pw.write(prevline + slide.dropRight(slide.length - args(1).toInt + 1) + "\n")
						}
						prevline = slide
					}
				} else {
					//pw.write(prevline + "\n")
					full_sequence += prevline + "\n"
					full_sequence.saveAsTextFile("intermediate_data/" + fasta_id.drop(1).filterNot(_ == ' ').replace(':', '_').replace('|', '_') + ".txt")
					//println(full_sequence)
					//pw.close
					fasta_id = line
					//pw = new PrintWriter(new File(path+fasta_id.drop(1).filterNot(_ == ' ').replace(':', '_').replace('|', '_') + ".txt"))
					prevline = ""
				}
				lineNum += 1
			}
		//pw.close
		} catch {
		case ex: FileNotFoundException => println("Couldn't find that file.")
		case ex: IOException => println("Had an IOException trying to read that file")
		}
		full_sequence += prevline
		full_sequence.saveAsTextFile("intermediate_data/" + fasta_id.drop(1).filterNot(_ == ' ').replace(':', '_').replace('|', '_') + ".txt")
		//println(full_sequence)
		//pw.close

	}
}
