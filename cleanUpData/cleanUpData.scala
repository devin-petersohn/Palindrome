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

		try {
			for (line <- Source.fromFile(filename).getLines()) {
				bigString += line
			}

			for( line <- bigString.sliding(200,200)){
				if(!line.charAt(0).equals('>')) { 
					if(!prevline.charAt(0).equals('>')) {
						if(!prevline.equals("")) {
							println(prevline + line.dropRight(line.length - args(1).toInt + 1))
							pw.write(prevline + line.dropRight(line.length - args(1).toInt + 1) + "\n")
						}
					}
				} else {
					pw.close
					fasta_id = line
					pw = new PrintWriter(new File(fasta_id.drop(1).filterNot(_ == ' ') + ".txt"))
				}
				prevline = line
			}
			pw.write(prevline + "\n")

			println(prevline)
		} catch {
			case ex: FileNotFoundException => println("Couldn't find that file.")
			case ex: IOException => println("Had an IOException trying to read that file")
		}

		pw.close

	}
}