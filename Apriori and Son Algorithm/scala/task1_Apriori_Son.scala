import org.apache.spark.{SparkConf, SparkContext}
import scala.util.Sorting
import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.io._
import org.json4s._
import scala.util.parsing.json.JSONObject


object task1 {

  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val n = 2 // partition_num
//    val input_file = "/Users/jaeyoungkim/IdeaProjects/hw2/src/main/scala/small1.csv"
//    val output_file = "task1.txt"
//    val c = 1
//    val support = 4

    // get argument
    val c = args(0).toInt
    val support = args(1).toInt
    val input_file = args(2)
    val output_file = args(3)


    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
    val textRDD = sc.textFile(path = input_file, n)
    val header = textRDD.first() // remove header
    val tmp = textRDD.filter(x => x != header)
    val baskets = if (c == 1) {tmp.map(x => x.split(",")).map(x => (x(0),List(x(1)))).reduceByKey(_++_).mapValues(x => x.toSet).map(x => x._2)} else {tmp.map(x => List(x.split(",")(1), x.split(",")(0))).map(x => (x(0),List(x(1)))).reduceByKey(_++_).mapValues(x => x.toSet).map(x => x._2)}
    val basket_list = baskets.collect()
    val itemset = basket_list.flatten.toSet
    val entire_size = basket_list.length


    // Phase 1

    def merge_basket_2d(baskets : Iterator[Set[String]]): List[Set[String]] = {
      val a = baskets.toList
      var collected_basket : List[Set[String]] = List()
//        = List[List[String]]()
      for (basket <- a) collected_basket:+= basket
      return collected_basket
    }

    def scaled_down_support_threshold(basket_list : List[Any], support : Int, entire_size : Int): Int= {

      val partition_size = basket_list.length
      val ps_float = partition_size.toFloat / entire_size
      val ps = ps_float * support
      return ps.ceil.toInt
    }


    def count_singleton(baskets : List[Set[String]], itemset : Set[String], ps : Int): Set[String] = {
      var count_dict = scala.collection.mutable.Map[String, Int]()
      var count_s = baskets.flatten.groupBy(identity).mapValues(_.size)
      var k = count_s.filter(x => x._2 >= ps)
      return k.keySet

    }

    def generate_permutations(candi_list : List[List[String]], pair_size : Int): List[List[String]] = {
      var permu_list : List[List[String]] = List()
      if (candi_list.size>0){
        for ((itemset, idx) <- candi_list.slice(0,candi_list.size-1).zipWithIndex){
          for (itemset2 <- candi_list.slice(idx+1,candi_list.size)){
            if (itemset.slice(0,itemset.size-1) == itemset2.slice(0,itemset2.size-1)){
              var candi = (itemset :+ itemset2(itemset2.length - 1)).sorted(Ordering[String])
              if(!permu_list.contains(candi)){
                var check_list : List[List[String]] = candi.combinations(pair_size-1).toList.map(x => x.sorted(Ordering[String]))
                if (check_list.toSet.subsetOf(candi_list.toSet)) {
                  permu_list = permu_list :+ candi
                }
              }
            }
          }
        }
      }
      return permu_list
    }


    def apriori(partition : Iterator[Set[String]], support: Int, entire_size : Int): List[List[String]] = {
      val baskets = merge_basket_2d(partition)
      var total_candidate : List[List[String]] = List()

      // Singleton
      val ps = scaled_down_support_threshold(baskets, support, entire_size )
      val singleton = count_singleton(baskets, itemset, ps).toList.sorted(Ordering[String].reverse)
      var baskets_shrinked : List[Set[String]] = List()
      for (basket <- baskets) {
        baskets_shrinked ::= singleton.toSet.intersect(basket)
      }
      for (single <- singleton){
        total_candidate ::= List(single)
      }


      //Pair
      var pair_list = singleton.toList.combinations(2).toList.map(x => x.sorted).sortWith((a,b) => a.mkString(" ")<b.mkString(" "))
      var candi_list : List[List[String]] = List()
      for (pair <- pair_list) {
        if (baskets_shrinked.filter(basket => pair.toSet.subsetOf(basket)).size>=ps){
          candi_list = candi_list :+ pair
        }
      }
      for (pair <- candi_list){
        total_candidate :+= pair
      }
      var len_pair = 2
      //Over Triple
      while (candi_list.size>0){
        len_pair += 1
        var pairs_to_be_counted = generate_permutations(candi_list, len_pair)
        candi_list = List()
        for (pair <- pairs_to_be_counted) {
          if (baskets_shrinked.filter(basket => pair.toSet.subsetOf(basket)).size>=ps){
            candi_list = candi_list :+ pair
            total_candidate = total_candidate :+ pair
          }
        }
      }

//
//      }
//    return generate_permutations(List(List("1","2"),List("2","3"),List("1","3")), 3)
      return total_candidate
    }




    val intermediate = baskets.mapPartitions(x => apriori(x, support, entire_size).iterator).map(x => Tuple2(x, 1)).reduceByKey((a,b) => a)
    val candidate_itemsets : List[List[String]] = intermediate.collect().toList.sortBy(r => (r._1.length, r._1.mkString(" ") )).map(x => x._1)



    //Phase 2

    def Map2(basket : Set[String], candidate_itemsets : List[List[String]]): List[(List[String],Int)] ={
      var answer_list : List[(List[String],Int)] = List()
      for (candidate <- candidate_itemsets) {
        if (candidate.toSet.subsetOf(basket)) {
          answer_list = answer_list :+ (candidate, 1)
        }
      }
      return answer_list
    }

    val freq_itemsets = baskets.map(x => Map2(x, candidate_itemsets)).flatMap( x => x).reduceByKey(_+_).filter(x => x._2 >= support.toInt).map(x =>x._1).collect().sortBy(r => (r.length, r.mkString(" ") ))
    val max_len_candi = candidate_itemsets(candidate_itemsets.length - 1).length
    val max_len_freq = freq_itemsets(freq_itemsets.length - 1).length



    var answer_string : String = ""
    val file = new File(output_file)
    val bw = new BufferedWriter(new FileWriter(file))

    bw.write("Candidates:\n")
    for (i <- 1 to max_len_candi) {
      answer_string = ""
      for (candidate <- candidate_itemsets) {
        if (candidate.size == i) {
          answer_string = answer_string.concat("(")
          for (item <- candidate) {
            answer_string = answer_string.concat("'").concat(item).concat("'").concat(",")
          }

          answer_string = answer_string.substring(0, answer_string.size - 1)
          answer_string = answer_string.concat("),")

        }

      }
      answer_string = answer_string.substring(0, answer_string.size - 1)
      answer_string = answer_string.concat("\n\n")
      bw.write(answer_string)
    }

    bw.write("Frequent Itemsets:\n")
    for (i <- 1 to max_len_freq) {
      answer_string = ""
      for (candidate <- freq_itemsets) {
        if (candidate.size == i) {
          answer_string = answer_string.concat("(")
          for (item <- candidate) {
            answer_string = answer_string.concat("'").concat(item).concat("'").concat(",")
          }

          answer_string = answer_string.substring(0, answer_string.size - 1)
          answer_string = answer_string.concat("),")

        }

      }
      answer_string = answer_string.substring(0, answer_string.size - 1)
      answer_string = answer_string.concat("\n\n")
      bw.write(answer_string)
    }


    bw.close()
    val duration = ((System.nanoTime - t1) / 1e9d).toString
    println("Duration: ".concat(duration))


  }
}
