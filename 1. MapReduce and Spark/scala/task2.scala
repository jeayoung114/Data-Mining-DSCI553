import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.io.{BufferedWriter, File, FileWriter}
import scala.util.parsing.json.JSONObject


object task2 {
  def main(args: Array[String]): Unit = {

//     get argument
//    val review_filepath = "/Users/jaeyoungkim/IdeaProjects/hw1/src/main/scala/test_review.json"
    val review_filepath = args(0)
    val output_filepath = args(1)
    val n_partition = args(2)


    //F
    val t1 = System.nanoTime




    val spark = new SparkConf().setAppName("task2").setMaster("local[*]")
    val sc = new SparkContext(spark)

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



    def ftn5(line: String): String = {
      val data = parse(line)
      val date = data \ "business_id"
      val JString(business_id) = date
      return business_id
    }

    val textRDD = sc.textFile(path = review_filepath)
    val tmp6 = textRDD.map(line => (ftn5(line), 1)).reduceByKey(_ + _).map(x => (x._2, x._1))
    val result = tmp6.groupByKey().sortByKey(false).mapValues(v => v.toList.sortWith(_ < _)).flatMap(x => ftn4(x)).take(10)
    val duration1 = (System.nanoTime - t1) / 1e9d
//    println(tmp6)
//    val n_items_1 = tmp6.foreachPartition(a => a.count(a => true))
    val n_partition_1 = tmp6.getNumPartitions
    val n_items_1 = tmp6.mapPartitions(iter => Iterator(iter.size), true).collect()






    //Customized
    val t2 = System.nanoTime

//    def custom_partitioner(key : String): Int = {
//      val first_element =key(0)
//      val ord = first_element
//      return ord.toInt
//    }


    val textRDD2 = sc.textFile(path = review_filepath)
    val tmp7 = textRDD2.map(line => (ftn5(line), 1)).partitionBy(new HashPartitioner(n_partition.toInt)).reduceByKey(_ + _).map(x => (x._2, x._1))
    val result2 = tmp7.groupByKey().sortByKey(false).mapValues(v => v.toList.sortWith(_ < _)).flatMap(x => ftn4(x)).take(10)

    val duration2 = (System.nanoTime - t2) / 1e9d
    println(duration2)

    val n_partition_2 = tmp7.getNumPartitions
    val n_items_2 = tmp7.mapPartitions(iter => Iterator(iter.size), true).collect()




    var n_items_1_ : List[Any] = List()
    var n_items_2_ : List[Any] = List()
    for ( w <- 0 to n_partition_1.toInt-1){
      n_items_1_ = n_items_1_ :+ n_items_1(w)
    }
    for ( w <- 0 to n_partition_2.toInt-1){
      n_items_2_ = n_items_2_ :+ n_items_2(w)
    }


    val answer_1 = JSONObject(Map("n_partition" -> n_partition_1, "n_items" -> n_items_1_, "exe_time" -> duration1))
    val answer_2 = JSONObject(Map("n_partition" -> n_partition_2, "n_items" -> n_items_2_, "exe_time" -> duration2))
    val answer = Map("default" -> answer_1,"customized" -> answer_2)

    val answer_string = JSONObject(answer).toString()
    println(answer_string)
    val answer_string_revised = answer_string.replace('(', '[').replace(')',']').replace("->",":").replaceAll("List", "").replaceAll("Map", "")


    // FileWriter
    val file = new File(output_filepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(answer_string_revised)
    bw.close()


  }
}