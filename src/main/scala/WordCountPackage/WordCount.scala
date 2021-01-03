package WordCountPackage

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.IntWritable

object WordCount {
  val inputFile = "src/main/resources/input/test.txt"
  //val outputFile = "output/output.txt"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("My App Word Count")
    // create sparkcontext
    val sc = new SparkContext(conf)

    // Load and parse the data file

    val inputRDD: RDD[String] = sc.textFile(args(0))
    println(s"Total lines: ${inputRDD.count()}")

    val contentArr: Array[String] = inputRDD.collect()
    println("content:")
    contentArr.foreach(println)


    val words: RDD[String] = inputRDD.flatMap(line => line.split(" "))
    val count1PerWords: RDD[(String, Int)] = words.map(word => (word, 1))
    val counts: RDD[(String, Int)] = count1PerWords.reduceByKey{ case (counter, nextVal) => counter + nextVal}

    //FileUtils.deleteDirectory(new File(outputFile))

    // save file
    counts.saveAsTextFile(args(1))
    println("Program executed successfully")



  }

}
