
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object RiobusReport {

	val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)

	def speedLimit(speed: Int) {
		// val filename = "./resources/abc.txt"
		val filename = "/Users/cassiohg/Downloads/riobusData/estudo_cassio_part_000000000000.csv"

		val file = sc.textFile(filename, 2).cache()
		//	file.getLines
		// 	.drop(1)
		// 	.toList
		.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
		.map(x => x.split(",").toList)
		.filter(x => x(5).nonEmpty)
		.filter(x => x(5).toFloat > speed)
		.take(10)
		.foreach(println)
		
	}

	def main(args: Array[String]) {
		
		speedLimit(60)
	}
}


