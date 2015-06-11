import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}


object RiobusReport {
	def main(args: Array[String]) {
		// val filename = "./resources/abc.txt"
		val filename = "/Users/cassiohg/Downloads/riobusData/estudo_cassio_part_000000000000.csv"
		try {
			val speeds = scala.io.Source.fromFile(filename)
				.getLines
				.drop(1)
				.toList
				.map(x => x.split(",").toList(5))
				.filter(_.nonEmpty)
				.map(_.toFloat)
				.filter(x => x >= 60.0)
				// .foldLeft(Map.empty[String, Int]){
				// 	(count, word) => count + (word -> (count.getOrElse(word, 0) + 1))
				// }
			println(speeds.size)
		} catch {
			case ex: FileNotFoundException => println("Couldn't find that file.")
			case ex: IOException => println("Had an IOException trying to read that file")
		}
	}
}
