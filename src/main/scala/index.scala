
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.io._

object RiobusReport {

	val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

    // method that will implement the speed limit report. It will filter every register that has speed above given value.
	def speedLimit(speed: Int) {
		val path = "/Users/cassiohg/Coding/Scala/riobus-report/" // path to project.

		val filename = path + "riobusData/estudo_cassio_part_000000000000.csv" // path to file.

		val filteredSpeeds = sc.textFile(filename, 2).cache() // reading text file with 2 copies, then caching on memory.
			.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } // droping first line of the RDD.
			.map(x => x.split(",").toList) // spliting each line by commas.
			.filter(x => x(5).nonEmpty) // removing lines with empty values in the 6th column.
			.filter(x => x(5).toFloat > speed) // filtering lines with value of 6th column, parsed to float, above 'speed'.

		

		// we will need the size of this result.
		val pw = new PrintWriter(new File(path + "result/speedLimit-resultCount.txt"), "UTF-8")
		pw.write(filteredSpeeds.count().toString) // printing the amount of lines left.
		pw.close

		// we will need some sample of the result, just to take a look at.
		val pw2 = new PrintWriter(new File(path + "result/speedLimit-result.txt"), "UTF-8")
		filteredSpeeds.take(10).foreach(x => pw2.write((x mkString ",") + "\n")) // printing the lines converted to strings.
		pw2.close
		
	}

	def main(args: Array[String]) {
		
		speedLimit(60)
	}
}


