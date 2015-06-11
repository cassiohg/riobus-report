import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}


object RiobusReport {
	def main(args: Array[String]) {
		val filename = "./resources/abc.txt"
		// val filename = "/Users/cassiohg/Downloads/riobusData/estudo_cassio_part_000000000000.csv"
		try {
			val speeds = scala.io.Source.fromFile(filename)
				.getLines
				.drop(1)
				.toList
				.map(x => x.split(",").toList)
				.filter(x => x(2).nonEmpty)
				.map(x => x(2).toFloat)
				// .filter(x => x(2) >= 10)
			println(speeds.take(10))
		} catch {
			case ex: FileNotFoundException => println("Couldn't find that file.")
			case ex: IOException => println("Had an IOException trying to read that file")
		}
	}
}
