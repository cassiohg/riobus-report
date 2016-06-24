
import java.io._
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

object TopSpeed {
	val conf = new SparkConf().setAppName("TopSpeed")//.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

	val dateFormatGoogle = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'") // format used by the data we have.
	val dateFormathttp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss") // format we use inside http message.

	def main(args: Array[String]) {
		// path to files being read.
		val filenameAndPath = "hdfs://localhost:8020/riobusData/estudo_cassio_part_00000000000[0-2]*"
		// path to file that will be written.
		val resultFilenameAndPath = "~/speedLimit-result.txt"

		val speed = args(0).toDouble // value that will hold the speed we will compare to every register speed value.
		val dateBeggin = dateFormathttp.parse(args(1)) // value date that will hold the beginning of the date interval.
		val dateEnd = dateFormathttp.parse(args(2)) // value date that will hold the end of the date interval.

		// rectangle specified by an anchor point, a length and a height. the anchor point is in the top left corner.
		val latitude1 = args(3).toDouble // value date that will hold the rectangle's bottom left latitude.
		val longitude1 = args(4).toDouble // value date that will hold the rectangle's bottom left longitude.
		val latitude2 = args(5).toDouble // value date that will hold rectangle's top right latitude.
		val longitude2 = args(6).toDouble // value date that will hold rectangle's top right longitude.
		val sampleLength = args(7).toInt // value that holds the amount of registers that will be saved on file.
		
		// Defining functions instead of methods. I am doing it because 'myApp' is an object, not a class, so I don't have
		// a constructor for it. That means I can't use the arguments 'args' out of 'main' method.

		// returns true if 'speedString' is not emmpty and it's value converted to float is bigger than 'speed' (arg(0)).
		val isAboveSpeed = {(speedString: String) => speedString.nonEmpty && speedString.toFloat > speed}

		// returns true if 'stringDate', converted to Date object is bigger than 'dateBeggin' and smaller than 'dateEnd'.
		val isdateInsideInterval = {(stringDate: String) =>
			//converting string to a date using pattern inside 'dateFormatGoogle'.
			var date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(stringDate)
			// appearenlty I can't use the 'dateFormatGoogle' because 'SimpleDateFormat' is not thread safe. I need a
			// new instance for every thread, but I don't know how to do it in an spark application. That's why I create a
			// new instance inside every function call.

			// testing if date is inside date interval.
			date.compareTo(dateBeggin) >= 0 && date.compareTo(dateEnd) <= 0
		}

		// returns true if latitude and longitude are inside the rectangle defined by the 
		// arguments given to this application.
		val isInsideRectangle = {(stringX: String, stringY: String) =>
			var lat = stringX.toDouble // converting string to double.
			var lng = stringY.toDouble // converting string to double.
			// testing if lat and longi are inside ractangle boundaries.
			lat >= latitude1 && lng >= longitude1 && lat <= latitude2 && lng <= longitude2
		}


		// Code that will implement the speed limit report. It will filter every register that has speed above given value,
		// inside a date interval and is inside a rectangle area in latitute and longitude.

		// reading text file with 2 copies, then caching on memory.
		val filteredSpeeds = sc.textFile(filenameAndPath, 2).cache()
			//dropping first line of the RDD.
			.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter } 
			// spliting each line by commas.
			.map(x => x.split(",").toList) 
			// filtering lines with value of 6th column above 'speed' that are not empty, are inside date interval
			// and rectangle area given in application argument.
			.filter(x => isAboveSpeed(x(5)) && isdateInsideInterval(x(0)) && isInsideRectangle(x(3), x(4)) )

		// we will need to write in a file the argument we have received (as a confirmation), the size of the result and 
		// a sample of it, of a small size.
		val pw = new PrintWriter(new File(resultFilenameAndPath), "UTF-8") // creating file to be written on.
		// writing the arguments we have received. just to give a feedback.
		pw.write(args(0)+","+args(1)+","+args(2)+","+args(3)+","+args(4)+","+args(5)+","+args(6)+","+args(7)+ "\n")
		// writing the amount of records in the result.
		pw.write(filteredSpeeds.count().toString + "\n")

		// each element of 'filteredSpeeds' needs to be concatenated and converted to string before being written in file.
		filteredSpeeds.take(sampleLength).foreach(x => pw.write((x mkString ",") + "\n")) // writing the first records.
		pw.close // closing file.
	}
}