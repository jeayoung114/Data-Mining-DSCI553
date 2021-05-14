import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io.{BufferedWriter, File, FileWriter}
import scala.util.parsing.json.JSONObject
// My prediction: My Scala code will run faster then my Spark code on a large dataset on Vocareum:
//
// Reason : The grading dataset is about 5GB and it is smaller than recent main memory size.
// When I run code on Scala, it will store all the items into the main memory and sort them
// However, when I run code with Spark, as the items are divided into multiple partitions, it has to do reshuffling process to sort the data.
// This will make Scala Code faster than Spark code.

object task3 {
  def main(args: Array[String]): Unit = {

    //     get argument
    //    val review_filepath = "/Users/jaeyoungkim/IdeaProjects/hw1/src/main/scala/test_review.json"

    val review_filepath = args(0)
    val business_filepath = args(1)
    val output_filepath_question_a = args(2)
    val output_filepath_question_b = args(3)


    def reduce_ftn(first : (Int, Double), second: (Int, Double)):(Int, Double)={
      return (first._1 + second._1, first._2 + second._2)
    }
    def map_ftn(x : (String, (Int, Double))): (Double, String)={
      return (x._2._2/x._2._1, x._1)  //avg, count
    }

    def ftn_(line: (Double, List[String])): List[(String, Double)] = {
      val num = line._1
      val list_of_names = line._2
      var res: List[(String, Double)] = List()
      for (w <- 0 to list_of_names.size - 1) {
        //        println((num, list_of_names(w)))
        //        res :+ (num, list_of_names(w))
        res = res :+ ((list_of_names(w), num))
      }
      return res
    }
    def ftn2(line: String, col_name : String): String = {
      val data = parse(line)
      val date = data \ col_name
      val JString(res) = date
      return res
    }
    def ftn4(line: String, col_name : String): Double = {
      val data = parse(line)
      val date = data \ col_name
      val JDouble(res) = date
      return res.toDouble
    }

    val t2 = System.nanoTime
    val spark = new SparkConf().setAppName("task3").setMaster("local[*]")
    val sc = new SparkContext(spark)

    val sample1RDD = sc.textFile(review_filepath)
    val sample2RDD = sc.textFile(business_filepath)

    //m1
    val t1 = System.nanoTime
    val sample1RDD1 = sc.textFile(review_filepath)
    val sample2RDD1 = sc.textFile(business_filepath)


    val tmp1 = sample1RDD1.map(line => (ftn2(line, "business_id"), ftn4(line, "stars")))
    val tmp21 = sample2RDD1.map(line => (ftn2(line, "business_id"), ftn2(line, "city")))

    val joined1 = tmp1.join(tmp21)

    val joined21 = joined1.map(line => (line._2._2,(1,line._2._1)))

    println(joined21.collect()(1))

    val joined31 = joined21.reduceByKey(reduce_ftn)





    val question_b_tmp = joined31.map(x => map_ftn(x)).collect()
    println(question_b_tmp(1))

    val question_b_tmp_1 = question_b_tmp.sortBy(question_b_tmp => (-question_b_tmp._1, question_b_tmp._2)).slice(0,10)
    val duration1 = (System.nanoTime - t1) / 1e9d

    ////////////////

    val tmp = sample1RDD.map(line => (ftn2(line, "business_id"), ftn4(line, "stars")))
    val tmp2 = sample2RDD.map(line => (ftn2(line, "business_id"), ftn2(line, "city")))
    println(tmp2.collect())
    println("################################################")
    val joined = tmp.join(tmp2)

    println("################################################")
    val joined2 = joined.map(line => (line._2._2,(1,line._2._1)))

    println(joined2.collect()(1))



    val joined3 = joined2.reduceByKey(reduce_ftn)



    val question_a_tmp = joined3.map(x => map_ftn(x)).groupByKey().sortByKey(false).mapValues(v => v.toList.sortWith(_ < _)).flatMap(x => ftn_(x)).take(10)
    println(question_a_tmp(1))
    val duration2 = (System.nanoTime - t2) / 1e9d
    println(duration2)

    val file = new File(output_filepath_question_a)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("city, stars"+"\n")
    for (w <- 0 to question_a_tmp.size-1){

      bw.write(question_a_tmp(w)._1 +','+ question_a_tmp(w)._2.toString+ "\n")

    }
    bw.close()

    val answer = Map("m1" -> duration1, "m2" -> duration2)

    // FileWriter
    val file1 = new File(output_filepath_question_b)
    val bw1 = new BufferedWriter(new FileWriter(file1))
    bw1.write(JSONObject(answer).toString())
    bw1.close()


  }
}