
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.io._

object RiobusReport {

	val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

	def speedLimit(speed: Int) {
		// val filename = "./resources/abc.txt"
		val filename = "/Users/cassiohg/Downloads/riobusData/estudo_cassio_part_000000000000.csv"

		val filteredSpeeds = sc.textFile(filename, 2).cache()
		//	file.getLines
		// 	.drop(1)
		// 	.toList
			.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
			.map(x => x.split(",").toList)
			.filter(x => x(5).nonEmpty)
			.filter(x => x(5).toFloat > speed)

		val pw = new PrintWriter(new File("/Users/cassiohg/Downloads/speedLimit-resultCount.txt"), "UTF-8")
		pw.write(filteredSpeeds.count().toString)
		pw.close

		val pw2 = new PrintWriter(new File("/Users/cassiohg/Downloads/speedLimit-result.txt"), "UTF-8")
		filteredSpeeds.take(10).foreach(x => pw2.write((x mkString ", ") + "\n"))
		pw2.close
		
	}

	def main(args: Array[String]) {
		
		speedLimit(60)
	}
}


