/*
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkMultipleStreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[3]") .setAppName("WordCountMultipleStreaming")

    val ssc = new StreamingContext(conf, Seconds(30))

    val source1DS = ssc.socketTextStream("localhost", 9999)
    val source2DS = ssc.socketTextStream("localhost", 8888)

    val combinedDS= source1DS.union(source2DS)

    val words = combinedDS.flatMap(_.split(","))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}
*/
