/*
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.util.UUID

case class Student(name: String, totalMarks: Int, schoolName: String, state: String, email: String)

object Student {
  def parseStringToStudent(line: String): Option[Student] = {
    println("parseStringToStudent(-):" + line)
    val dataArr = line.split(",")
    if (dataArr.length == 5) {
      Some(Student(dataArr(0), Integer.parseInt(dataArr(1)), dataArr(2), dataArr(3), dataArr(4)))
    }
    else None
  }
}

object SparkStreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("Student Processor")
    val ssc = new StreamingContext(conf, Seconds(30))
    val inputDSStream = ssc.textFileStream("file:///home/hduser/studentDataNew")

    val studentDSStream = inputDSStream.flatMap(line => {
      println("Process student")
      Student.parseStringToStudent(line)
    })

    val studentMarksGT500 = studentDSStream.filter(student => {
      println("Filtering logic :" + student)
      student.totalMarks > 500
    })
    studentMarksGT500.print
    studentMarksGT500.saveAsTextFiles(s"hdfs://localhost:9000/user/hduser/studentDataOutput")

    /*studentMarksGT500.foreachRDD(rdd=>{
      val uuid=UUID.randomUUID()
      if(rdd.count()!=0){
        rdd.saveAsTextFile(s"hdfs://localhost:9000/user/hduser/studentDataOutput_$uuid")
      }
    })*/
    ssc.start() // Start the computation
    ssc.awaitTermination()
  }
}
*/
