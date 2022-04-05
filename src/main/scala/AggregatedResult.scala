import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object SparkStreamingWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("My App")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))

    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    inputData += sc.makeRDD(List("apple","ball","apple","ball","apple")) //1st
    inputData += sc.makeRDD(List("apple","apple","ball","ball")) //2nd
    inputData += sc.makeRDD(List("apple", "ball", "ball", "ball")) //3nd
    inputData += sc.makeRDD(List("apple", "ball", "ball", "ball")) //4th
    inputData += sc.makeRDD(List("cat", "cat", "cat", "cat")) //5th
    inputData += sc.makeRDD(List("apple", "apple", "ball", "ball")) //6th
    inputData += sc.parallelize(List("apple", "apple", "apple", "ball")) //7th
    inputData += sc.parallelize(List("apple", "apple", "ball", "apple")) //8th
    val inputStream: InputDStream[String] = ssc.queueStream(inputData)
    //windowDuration,slideDuration
    //val inputStream: InputDStream[String] = ssc.queueStream(inputData)
    val combinedData = inputStream.reduceByWindow(_+_,Seconds(20), Seconds(10))
    combinedData.print



    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}
/*

No processing
(apple,5) (ball,4) 14:40 15:00
No processing
(apple,4) (ball,8)
No processing
(apple,3) (cat,4) (ball,5)
No processing
(apple,8) (ball,4)
No processing
(apple,3) (ball,1)
No processing
Processed but no Output



 */