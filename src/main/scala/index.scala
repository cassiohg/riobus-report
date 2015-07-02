
import java.io._

import org.apache.spark.{SparkConf, SparkContext}

object RiobusReport {

	val conf = new SparkConf().setAppName("RiobusReport").setMaster("spark://JonnyLaptop:7077")
    val sc = new SparkContext(conf)

    // method that will implement the speed limit report. It will filter every register that has speed above given value.
	def speedLimit(speed: Int) {
		val path = "/mnt/jonny-data/Documentos/UFRJ/BigData/riobus-data/" // path to project.

		val filename = path + "estudo_cassio_part_000000000000.csv" // path to file.

		val filteredSpeeds = sc.textFile(filename, 2).cache() // reading text file with 2 copies, then caching on memory.
			.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } //droping first line of the RDD.
			.map(x => x.split(",").toList) // spliting each line by commas.
			.filter(x => x(5).nonEmpty) // removing lines with empty values in the 6th column.
			.filter(x => x(5).toFloat > speed) // filtering lines with value of 6th column, parsed to float, above 'speed'.



		// we will need to write in a file the argument we have received (as a confirmation), the size of the result and 
		// a sample of it, of a small size.
		val pw = new PrintWriter(new File(path + "result/speedLimit-result.txt"), "UTF-8") // creating file.
		pw.write(speed.toString + "\n") // writing argument we have received.
		pw.write(filteredSpeeds.count().toString + "\n") // writing the amount of records in the result.

		// each element of 'filteredSpeeds' needs to be concatenated and converted to string before being written in file.
		filteredSpeeds.take(10).foreach(x => pw.write((x mkString ",") + "\n")) // writing the first 10 records.
		pw.close // closing file.
	}

	def main(args: Array[String]) {
		speedLimit(args(0).toInt) // reading speed from arguments
	}
}
