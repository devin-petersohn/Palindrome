import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}
import java.io._

object Cleanup {
	def main(args: Array[String]) = {


		val filename = args(0)
		var pw = new PrintWriter(new File("temporary_fasta_file.txt"))
		var prevline = ""
		var fasta_id = ""
		var bigString = ""
		var lineNum = 0
		var emptyCounter = true
		var filecontents = new ArrayBuffer[String]

		try {
			for (line <- Source.fromFile(filename).getLines()) {
				if(line.charAt(0) != '>') {
					if(emptyCounter == true){
						filecontents += line
						emptyCounter = false
					} else filecontents(lineNum) += line
				} else {
					lineNum += 1
					filecontents += line
					emptyCounter = true
				}
			}

			lineNum = 1
			prevline = ""
			for (line <- filecontents) {
				if(lineNum%2 == 0) {
					for(slide <- line.sliding(200,200)){
						if(!prevline.equals("")) {
							pw.write(prevline + slide.dropRight(slide.length - args(1).toInt + 1) + "\n")
							//println(prevline + slide.dropRight(slide.length - args(1).toInt + 1))
						}
						prevline = slide
					}
				} else {
					pw.write(prevline + "\n")
					//println(prevline)
					pw.close
					fasta_id = line
					pw = new PrintWriter(new File(fasta_id.drop(1).filterNot(_ == ' ').replace(':', '_') + ".txt"))
					prevline = ""
				}
				lineNum += 1
				pw.write(prevline + "\n")
				//println(prevline)
			}
			pw.close

		} catch {
			case ex: FileNotFoundException => println("Couldn't find that file.")
			case ex: IOException => println("Had an IOException trying to read that file")
		}

		pw.close

	}
}
