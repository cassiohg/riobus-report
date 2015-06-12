import scala.io.Source
import java.io.{FileReader, FileNotFoundException, IOException}


object RiobusReport {

	def speedLimit(speed: Int) {
		// val filename = "./resources/abc.txt"
		val filename = "/Users/cassiohg/Downloads/riobusData/estudo_cassio_part_000000000000.csv"
		try {
			val speedsFiltered = scala.io.Source.fromFile(filename)
				.getLines
				.drop(1)
				.toList
				.map(x => x.split(",").toList)
				.filter(x => x(5).nonEmpty)
				.filter(x => x(5).toFloat > speed)
				.take(10)
				.foreach(println)
		} catch {
			case ex: FileNotFoundException => println("Couldn't find that file.")
			case ex: IOException => println("Had an IOException trying to read that file")
		}
	}

	def main(args: Array[String]) {
		speedLimit(60)
	}
}
