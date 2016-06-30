#riobus-report-top-speed

this is a spark application that reads bus registers from csv files saved on hadoop and filters registers that are above a given speed, inside a given rectangle and inside a given date interval. As output, it will write to a file locally, which path is also given as argument to this application, all the filtered registers (and also the filter arguments in the first line, so filtered registers are written from second line forwards).

<h6>you only need to produce a jar</h6>
<ol>
    <li>install sbt (simple build tool)<br>
    more information in http://www.scala-sbt.org/0.13/tutorial/Setup.html<br>
    <li>cd to project folder <br>
    <code>$ cd path/to/project</code></li>
    <li>use pacakge command with sbt <br>
    <code>$ sbt package</code></li>
    <li>jar will be inside ./target/scala-2.10/ folder<br>
</ol>

<h6>now you need to submit this jar to spark<br></h6>
read <https://spark.apache.org/docs/latest/submitting-applications.html> for more information<br>
this project has been tested on spark 1.6.1
you can submit this job like this

    path/to/spark-submit 
    --driver-memory 1536m 
    --class "<class name>" 
    --master local[*] 
    path/to/project/jar
    <out file>
    <date begin>
    <date end> 
    <lat1>
    <long1>
    <lat2> 
    <long2>
    <speed>
    <output length>

where
	
* `<class name>` is the projects main class name.
* `<out file>` is the path to the output file this job will write it's result into.
* `<date begin>` is date in the format yyyy-MM-dd'T'HH:mm:ss (eg: 2015-04-14T13:00:00) that is the start of the interval.
* `<date end>` is date in the format yyyy-MM-dd'T'HH:mm:ss (eg: 2015-04-14T15:00:00) that is the end of the interval.
* `<lat1>` is the rectangles bottom left latitude value.
* `<long1>` is the rectangles bottom left longitude value.
* `<lat2>` is the rectangles top right latitude value.
* `<long2>` is the rectangles top right longitude value.
* `<speed>` is the speed we will use to filter buses. buses below that speed won't be on result.
* `<output length>` is the size of the sample written to output file. this prevents the application from writing too many results.