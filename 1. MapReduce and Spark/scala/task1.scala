import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io._
import org.json4s._
import scala.util.parsing.json.JSONObject


object task1{
  def main(args:Array[String]): Unit= {

    // get argument
    val review_filepath = args(0)
    val output_filepath = args(1)
//    val review_filepath = "/Users/jaeyoungkim/IdeaProjects/hw1/src/main/scala/test_review.json"

    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val textRDD = sc.textFile(path = review_filepath)


    //1a
    val tmp1 = textRDD.map(line => (1, 1)).reduceByKey(_ + _).collect()

    val n_review = tmp1(0)._2

    println(n_review)


    //1b

    def ftn1(line: String): Int = {
      val data = parse(line)
      val date = data \ "date"
      val JString(year_) = date
      val year = year_.substring(0, 4)
      if (year == "2018") {
        return 1;
      }
      else {
        return 0;
      }

    }


    val tmp2 = textRDD.map(line => (1, ftn1(line))).reduceByKey(_ + _).collect()
    val n_review_2018 = tmp2(0)._2

    println(n_review_2018)


    //1c

    def ftn2(line: String): String = {
      val data = parse(line)
      val date = data \ "user_id"
      val JString(user_id) = date
      return user_id
    }

    (1, 1)

    def ftn3(x: Int, y: Int): Int = {
      return 1
    }

    val tmp3 = textRDD.map(line => (ftn2(line), 1)).reduceByKey(ftn3).map(x => (1, 1)).reduceByKey(_ + _).collect()
    val n_user = tmp3(0)._2

    println(n_user)

    //1d


    def ftn4(line: (Int, List[String])): List[(String, Int)] = {
      val num = line._1
      val list_of_names = line._2
      var res: List[(String, Int)] = List()
      for (w <- 0 to list_of_names.size - 1) {
        //        println((num, list_of_names(w)))
        //        res :+ (num, list_of_names(w))
        res = res :+ ((list_of_names(w), num))
      }
      return res
    }

    val tmp4 = textRDD.map(line => (ftn2(line), 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).groupByKey().sortByKey(false).mapValues(v => v.toList.sortWith(_ < _)).flatMap(x => ftn4(x)).take(10)
    //    println(tmp4)
    var top10_user: List[Any] = List()
    for (w <- 0 to tmp4.size - 1) {
      top10_user = top10_user :+ ("\""+tmp4(w)._1+"\"", tmp4(w)._2)
    }
    println(top10_user)


    //1e


    def ftn5(line: String): String = {
      val data = parse(line)
      val date = data \ "business_id"
      val JString(business_id) = date
      return business_id
    }

    def ftn6(x: Int, y: Int): Int = {
      return x
    }

    val tmp5 = textRDD.map(line => (ftn5(line), 1)).reduceByKey(ftn6).map(x => (1, 1)).reduceByKey(_ + _).collect()
//    println(tmp5)
    val n_business = tmp5(0)._2
//    println(n_business)


    //    1f


    val tmp6 = textRDD.map(line => (ftn5(line), 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).groupByKey().sortByKey(false).mapValues(v => v.toList.sortWith(_ < _)).flatMap(x => ftn4(x)).take(10)
//    println(tmp6)
    var top10_business: List[Any] = List()
    for (w <- 0 to tmp6.size - 1) {
      top10_business = top10_business :+ ("\""+tmp6(w)._1+"\"", tmp6(w)._2)
    }

    println(top10_user)

    val answer = Map("n_review" -> n_review, "n_review_2018" -> n_review_2018, "n_user" -> n_user, "top10_user" -> top10_user, "n_business" -> n_business, "top10_business" -> top10_business)


    val answer_string = JSONObject(answer).toString()
//    println(answer)
    val answer_string_revised = answer_string.replace('(', '[').replace(')',']').replaceAll("List", "")
    // FileWriter



    val file = new File(output_filepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(answer_string_revised)
    bw.close()





  }
}
