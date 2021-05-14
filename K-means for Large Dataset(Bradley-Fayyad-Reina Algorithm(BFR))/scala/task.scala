import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import breeze.linalg._
import breeze.numerics.sqrt
import scala.math._
import java.io._


object task {

  def main(args: Array[String]): Unit = {

    val t1 = System.nanoTime
    val n = 10 // partition_num


    // get argument
    val input_file = args(0)
    val n_cluster = args(1).toInt
    val output_file = args(2)


    val spark = new SparkConf().setAppName("task1").setMaster("local[*]")
    val sc = new SparkContext(spark)
    sc.setLogLevel("ERROR")

    val random_object = scala.util.Random

    def random_split_5(): Int = {
      val rand_num: Float = random_object.nextFloat()
      if (rand_num < 0.2) {
        return 0
      }
      else if (rand_num < 0.4) {
        return 1
      }
      else if (rand_num < 0.6) {
        return 2
      }
      else if (rand_num < 0.8) {
        return 3
      }
      else {
        return 4
      }
    }

    //    RDDS
    val textRDD = sc.textFile(path = input_file, n)

    //    Step 1. Load 20% of the data randomly.
    val random_data = textRDD.map(x => for (feature <- x.split(",")) yield feature.toDouble)
      .map(x => (x, random_split_5()))
      .cache()

    val random_data_label = random_data.map(x => x._1).map(x=> (x.slice(2, x.length), x(0))).cache()
    val random_dense_vec_label = random_data_label.map(x => (Vectors.dense(x._1),x._2)).collect()
    val random_dense_vec_label_breeze = random_data_label.map(x => (DenseVector(x._1),x._2)).collect()
    var vec_to_id = scala.collection.mutable.Map[org.apache.spark.mllib.linalg.Vector, Int]()
    var vec_to_id2 = scala.collection.mutable.Map[DenseVector[Double], Int]()
    for ((row, label) <- random_dense_vec_label_breeze) vec_to_id2 += (row -> label.toInt)
    for ((row, label) <- random_dense_vec_label) vec_to_id += (row -> label.toInt)

    var random_data_0 = random_data.filter(x => x._2 == 0).map(x => x._1).cache()
    var Round = 1
    val dim: Int = random_data_0.take(1)(0).length - 2

    //    Step 2. Run K-Means (e.g., from sklearn) with a large K (e.g., 5 times of the number of the input clusters) on the data in memory using the Euclidean distance as the similarity measurement.
    var data = random_data_0.map(x => x.slice(2, x.length)).map(x => Vectors.dense(x))

    val numIterations = 1
    val kmeans1 = KMeans.train(data, n_cluster * 20, numIterations)
    var cluster_result = kmeans1.predict(data).collect()
    var label_row_map1 = scala.collection.mutable.Map[org.apache.spark.mllib.linalg.Vector, Int]()
    var dat = data.collect()
    for ((vec, label) <- dat.zip(cluster_result)) label_row_map1 += (vec -> label)




    val data2 = random_data_0.map(x => x.slice(2, x.length))


    //    Step 3. In the K-Means result from Step 2, move all the clusters that contain only one point to RS (outliers).
    println("Step 3")
    var RS: ListBuffer[org.apache.spark.mllib.linalg.Vector] = ListBuffer()
    var RS_labels = sc.parallelize(cluster_result, n)
      .map(x => (x, 1))
      .reduceByKey(_ + _)
      .filter(x => x._2 == 1)
      .map(x => x._1).collect()

    for ((row, label) <- dat.zip(cluster_result)) {
      if (RS_labels.contains(label)) {
        RS += row
      }
    }

    //    Step 4. Run K-Means again to cluster the rest of the data points with K = the number of input clusters.
    println("Step 4")
    data = data.filter(x => !RS.contains(x)).cache()
    val kmeans2 = KMeans.train(data, n_cluster, numIterations)
    cluster_result = kmeans2.predict(data).collect()

    def kmeans_result(kmeans: KMeansModel, data: RDD[org.apache.spark.mllib.linalg.Vector]): scala.collection.Map[org.apache.spark.mllib.linalg.Vector, Int] = {
      var answer = scala.collection.mutable.Map[org.apache.spark.mllib.linalg.Vector, Int]()
      var cluster_result = kmeans.predict(data).collect()
      val dat = data.collect()
      for ((label_, idx_) <- cluster_result.zipWithIndex) answer += (dat(idx_) -> label_)
      return answer
    }


    //    add result to result Map
    var res = scala.collection.mutable.Map[Int, Int]()
    dat = data.collect()
    var ds = scala.collection.mutable.Map[org.apache.spark.mllib.linalg.Vector, Int]()
    for ((label, idx) <- cluster_result.zipWithIndex) res += (vec_to_id(dat(idx)) -> label)
    for ((label, idx) <- cluster_result.zipWithIndex) ds += (dat(idx) -> label)

    //    Step 5. Use the K-Means result from Step 4 to generate the DS clusters (i.e., discard their points and generate statistics).
    println("Step 5")

    //    define a function  getting N, SUM, SUMSQ from data

    def N_SUM_SUMSQ(kmeans_result: scala.collection.Map[org.apache.spark.mllib.linalg.Vector, Int]): (scala.collection.mutable.Map[Int, Int], scala.collection.mutable.Map[Int, DenseVector[Double]], scala.collection.mutable.Map[Int, DenseVector[Double]]) = {

      val tmp: List[Tuple2[org.apache.spark.mllib.linalg.Vector, Int]] = kmeans_result.toList


      val c = sc.parallelize(tmp, n).map(x => (x._2, 1)).reduceByKey(_ + _).collect()

      val d = sc.parallelize(tmp, n).map(x => (x._2, DenseVector(x._1.toArray))).reduceByKey(_ + _).collect()

      val e = sc.parallelize(tmp, n).map(x => (x._2, DenseVector(x._1.toArray))).map(x => (x._1, x._2 * x._2)).reduceByKey(_ + _).collect()


      var num = scala.collection.mutable.Map[Int, Int]()
      var summ = scala.collection.mutable.Map[Int, DenseVector[Double]]()
      var sumsqm = scala.collection.mutable.Map[Int, DenseVector[Double]]()

      for ((label, count) <- c) num += (label -> count)
      for ((label, sum) <- d) summ += (label -> sum)
      for ((label, sumsq) <- e) sumsqm += (label -> sumsq)

      return (num, summ, sumsqm)
    }

    val (n1, sum1, sumsq1) = N_SUM_SUMSQ(kmeans_result(kmeans2, data))




    //    Step 6. Run K-Means on the points in the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).
    println("Step 6")
    var RS_count = 0
    var CS_count = 0
    var CS = scala.collection.mutable.Map[org.apache.spark.mllib.linalg.Vector, Int]()
    var n_cluster_CS = 0
    var n_DS: Int = 0
    var (n_CS, sum_CS, sumsq_CS) = N_SUM_SUMSQ(CS)
    for ((label, count) <- n1.toList) {
      n_DS += count
    }
    if (RS.size != 0) {
      var kmeans3 = KMeans.train(sc.parallelize(RS), RS.size, numIterations)
      if (RS.size >= 2 * n_cluster) {
        kmeans3 = KMeans.train(sc.parallelize(RS), n_cluster * 2, numIterations)
      }
      cluster_result = kmeans3.predict(sc.parallelize(RS)).collect()
      n_cluster_CS = sc.parallelize(cluster_result, n)
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .map(x => (x._1, 1))
        .reduceByKey(_ + _)
        .map(x => x._1).collect()(0)
      RS_labels = sc.parallelize(cluster_result, n)
        .map(x => (x, 1))
        .reduceByKey(_ + _)
        .filter(x => x._2 == 1)
        .map(x => x._1).collect()
      var RS_new: ListBuffer[org.apache.spark.mllib.linalg.Vector] = ListBuffer()

      for ((row, label) <- RS.zip(cluster_result)) {
        if (RS_labels.contains(label)) {
          RS_count += 1
          RS_new :+= row
        }
        if (!RS_labels.contains(label)) {
          CS_count += 1
          CS += (row -> label)
        }
      }
    RS = RS_new

      var (n_CS, sum_CS, sumsq_CS) = N_SUM_SUMSQ(CS)
    }



    val file = new File(output_file)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("The intermediate results:\n")
    bw.write("Round %d: ".format(Round) + n_DS.toString + "," + CS.size.toString + "," + CS_count.toString + "," + RS_count.toString + '\n')
    println("Round %d: ".format(Round) + n_DS.toString + "," + CS.size.toString + "," + CS_count.toString + "," + RS_count.toString + '\n')



    for (round <- List(2,3,4,5)) {

      //    Step 7. Load another 20% of the data randomly.
      println("Step 7")
      var random_data_round = random_data.filter(x => x._2 == round - 1).map(x => x._1).cache()
      var random_data_changed = random_data_round.map(x => x.slice(2, x.length)).map(x => DenseVector(x)).cache()




      def Mahalanobis_Distance(x: DenseVector[Double], nm: scala.collection.Map[Int, Int], summ: scala.collection.Map[Int, DenseVector[Double]], sumsqm: scala.collection.Map[Int, DenseVector[Double]]): scala.collection.Map[Int, Double] = {
        var d_to_cluster = scala.collection.Map[Int, Double]()
        for (cluster_idx <- nm.keys) {
          var c_i = summ(cluster_idx)/nm(cluster_idx).toDouble
          var sigma_i = sqrt(sumsqm(cluster_idx) / nm(cluster_idx).toDouble - (summ(cluster_idx) / nm(cluster_idx).toDouble)*(summ(cluster_idx) / nm(cluster_idx).toDouble))
          var d = math.pow((((x - c_i)/sigma_i)* ((x - c_i)/sigma_i)).reduce(_+_),0.5)

          d_to_cluster += (cluster_idx -> d)
        }
        return d_to_cluster
      }
      def Mahalonobis_Distance_btw_two_clusters(cluster_id : Int, nm: scala.collection.Map[Int, Int], summ: scala.collection.Map[Int, DenseVector[Double]], sumsqm: scala.collection.Map[Int, DenseVector[Double]]): scala.collection.Map[Int, Double] = {
        var d_to_cluster = scala.collection.Map[Int, Double]()
        var c_n = nm(cluster_id)
        var c_sum = summ(cluster_id)
        var c_sumsq = sumsqm(cluster_id)

        var c_centroid = summ(cluster_id)/nm(cluster_id).toDouble
        var c_sigma = sqrt(sumsqm(cluster_id)/nm(cluster_id).toDouble - summ(cluster_id)/nm(cluster_id).toDouble * summ(cluster_id)/nm(cluster_id).toDouble)

        for (cluster_idx <- nm.keys){
          var tmp = 0

          var c_i = summ(cluster_idx)/nm(cluster_idx).toDouble
          var sigma_i = (sumsqm(cluster_idx)/nm(cluster_idx).toDouble) -sqrt( (summ(cluster_idx) / nm(cluster_idx).toDouble) * (summ(cluster_idx) / nm(cluster_idx).toDouble))
          var d = math.pow(((c_centroid - c_i)*(c_centroid - c_i) / (c_sigma * sigma_i)).reduce(_+_),0.5)
          d_to_cluster += (cluster_idx -> d)

        }

        return d_to_cluster
      }

      def Mahalanobis_Distance_btw_CS_DS(cluster_id : Int, nm: scala.collection.Map[Int, Int], summ: scala.collection.Map[Int, DenseVector[Double]], sumsqm: scala.collection.Map[Int, DenseVector[Double]],nm_CS: scala.collection.Map[Int, Int], summ_CS: scala.collection.Map[Int, DenseVector[Double]], sumsqm_CS: scala.collection.Map[Int, DenseVector[Double]]): scala.collection.Map[Int, Double] = {
        var d_to_cluster = scala.collection.Map[Int, Double]()
        var c_n = nm_CS(cluster_id)
        var c_sum = summ_CS(cluster_id)
        var c_sumsq = sumsqm_CS(cluster_id)

        var c_centroid = summ_CS(cluster_id)/nm_CS(cluster_id).toDouble
        var c_sigma = sqrt(sumsqm_CS(cluster_id)/nm_CS(cluster_id).toDouble - summ_CS(cluster_id)/nm_CS(cluster_id).toDouble * summ_CS(cluster_id)/nm_CS(cluster_id).toDouble)

        for (cluster_idx <- nm.keys){
          var tmp = 0

          var c_i = summ(cluster_idx)/nm(cluster_idx).toDouble
          var sigma_i = (sumsqm(cluster_idx)/nm(cluster_idx).toDouble) -sqrt( (summ(cluster_idx) / nm(cluster_idx).toDouble) * (summ(cluster_idx) / nm(cluster_idx).toDouble))
          var d = math.pow(((c_centroid - c_i)*(c_centroid - c_i) / (c_sigma * sigma_i)).reduce(_+_),2)
          d_to_cluster += (cluster_idx -> d)

        }
        return d_to_cluster
      }

//      Step 8. For the new points, compare them to each of the DS using the Mahalanobis Distance and assign
//      them to the nearest DSclusters if the distance is <2 root 𝑑.
      println("Step 8")
      val base = 2 * ((math.pow(dim, 0.5)))
      var filtered_data = random_data_changed
        .map(x => (x, Mahalanobis_Distance(x, n1, sum1, sumsq1))) // vec, dist
        .map(x => (x._1, x._2.minBy(_._2))) // 1 : label 2 : dist
        .map(x => if (x._2._2 < base) (x._1, x._2._1) else (x._1, -1)).cache()



      //add result to res
      for ((row, label) <- filtered_data.collect()) {
        if (label != -1) {
          res += (vec_to_id2(row) -> label)
          ds += (Vectors.dense(row.toArray) -> label)
        }
      }


      var n_new = filtered_data.filter(x => x._2 != -1).map(x => (x._2, 1)).reduceByKey(_ + _).collect()
      var sum_new = filtered_data.filter(x => x._2 != -1).map(x => (x._2, x._1))
        .reduceByKey(_ + _).collect()
      var sumsq_new = filtered_data.filter(x => x._2 != -1).map(x => (x._2, x._1 * x._1))
        .reduceByKey(_ + _).collect()


      for ((label, count) <- n_new) {
        var tmp: Int = n1(label) + count
        n1.remove(label)
        n1 += (label -> tmp)
      }

      for ((label, vec) <- sum_new) {
        var tmp: DenseVector[Double] = sum1(label) + vec
        sum1.remove(label)
        sum1 += (label -> tmp)
      }

      for ((label, vec) <- sumsq_new) {
        var tmp: DenseVector[Double] = sumsq1(label) + vec
        sumsq1.remove(label)
        sumsq1 += (label -> tmp)
      }


//      Step 9. For the new points that are not assigned to DS clusters, using the Mahalanobis Distance and
//      assign the points to the nearest CS clusters if the distance is <2 root 𝑑
      println("Step 9")
      var not_ds: RDD[(DenseVector[Double], Int)] = filtered_data
      if (n_CS != scala.collection.mutable.Map[Int, Int]()) {
        println("345 if")
        not_ds = filtered_data.map(x => x._1)
          .map(x => (x, Mahalanobis_Distance(x, n_CS, sum_CS, sumsq_CS)))
          .map(x => (x._1, x._2.minBy(_._2))) // 1 : label 2 : dist
          .map(x => if (x._2._2 < base) (x._1, x._2._1) else (x._1, -1)).cache()

        var n_CS_new = not_ds.map(x => (x._2, 1)).reduceByKey(_ + _).collect()
        var sum_CS_new = not_ds.map(x => (x._2, x._1))
          .reduceByKey(_ + _).collect()
        var sumsq_CS_new = not_ds.map(x => (x._2, x._1 * x._1))
          .reduceByKey(_ + _).collect()


        for ((label, count) <- n_CS_new) {
          var tmp: Int = n1(label) + count
          n1.remove(label)
          n1 += (label -> tmp)
        }

        for ((label, vec) <- sum_CS_new) {
          var tmp: DenseVector[Double] = sum_CS(label) + vec
          sum_CS.remove(label)
          sum_CS += (label -> tmp)
        }

        for ((label, vec) <- sumsq_CS_new) {
          var tmp: DenseVector[Double] = sumsq_CS(label) + vec
          sumsq_CS.remove(label)
          sumsq_CS += (label -> tmp)
        }
      }

      else {
        not_ds = filtered_data.filter(x => x._2 == -1)
          .map(x => x._1)
          .map(x => (x, -1)).cache()
      }


      //      Step 10. For the new points that are not assigned to a DS cluster or a CS cluster, assign them to RS.
      println("Step 10")
      var not_cs = not_ds.filter(x => x._2 == -1)
        .map(x => x._1).collect()


      for (row <- not_cs) {
        RS += Vectors.dense(row.toArray)
      }

      //      Step 11. Run K-Means on the RS with a large K (e.g., 5 times of the number of the input clusters) to generate CS (clusters with more than one points) and RS (clusters with only one point).
      println("Step 11")
      if (RS.size != 0) {
        var kmeans4 = KMeans.train(sc.parallelize(RS), RS.size, numIterations)
        if (RS.size > 2 * n_cluster) {
          kmeans4 = KMeans.train(sc.parallelize(RS), n_cluster * 2, numIterations)
        }
        cluster_result = kmeans4.predict(sc.parallelize(RS)).collect()
        n_cluster_CS = sc.parallelize(cluster_result, n)
          .map(x => (x, 1))
          .reduceByKey(_ + _)
          .map(x => (x._1, 1))
          .reduceByKey(_ + _)
          .map(x => x._1).collect()(0)
        RS_labels = sc.parallelize(cluster_result, n)
          .map(x => (x, 1))
          .reduceByKey(_ + _)
          .filter(x => x._2 == 1)
          .map(x => x._1).collect()

        var rm_row: ListBuffer[org.apache.spark.mllib.linalg.Vector] = ListBuffer()
        for ((row, label) <- RS.zip(cluster_result)) {
          if (RS_labels.contains(label)) {
            RS_count += 1
          }
          else if (!RS_labels.contains(label)) {
            CS_count += 1
            var tmp: Int = label + round * 10000
            CS += (row -> tmp)
            rm_row += row

          }
        }

        println("RS.size before" ,RS.size)
        RS = RS.filter(!rm_row.contains(_))
        println("RS.size after" ,RS.size)
        var (n_CS, sum_CS, sumsq_CS) = N_SUM_SUMSQ(CS)

      }

      //      Step 12.Merge CS clusters that have a Mahalanobis Distance <2 root 𝑑.
      println("Step 12")
      var new_N_CS_len = 10 ^ 10
      var tot_CS : Int = 0
      for((label, count) <- n_CS) tot_CS+=count
      while (tot_CS != new_N_CS_len && new_N_CS_len > 1) {
        var (n_CS, sum_CS, sumsq_CS) = N_SUM_SUMSQ(CS)
        var centroids_CS = scala.collection.mutable.Map[Int, DenseVector[Double]]()
        for (cluster_id <- CS.values) {
          centroids_CS += (cluster_id -> sum_CS(cluster_id) / n_CS(cluster_id).toDouble)
        }
        var tmp = 0
        for (cluster_id <- centroids_CS.keys) {
          if (tmp == 0){
            var m_dist  = Mahalonobis_Distance_btw_two_clusters(cluster_id, n_CS, sum_CS, sumsq_CS)
            m_dist.-(cluster_id)
            var (closest_cluster, mindist) = m_dist.minBy(_._2)
            if (mindist < base){
              tmp+=1
              for ((row, label) <- CS){
                if (label == closest_cluster){
                  CS.remove(row)
                  CS(row) = cluster_id
                }
              }

            }
          }

        }
        new_N_CS_len = CS.size
      var (a, b, c) = N_SUM_SUMSQ(CS)
      n_CS = a
      sum_CS = b
      sumsq_CS = c

      }

//      Step 13. merge CS with DS that have a Mahalanobis Distance <2 root d

      if (round == 5){
        println("Step 13")
        var (a, b, c) = N_SUM_SUMSQ(CS)
        n_CS = a
        sum_CS = b
        sumsq_CS = c
        var del_cluster : List[Int] = List()
        for (cluster : Int <- CS.values.toSet){
          var dist_to_DS = Mahalanobis_Distance_btw_CS_DS(cluster, n1, sum1, sumsq1, n_CS, sum_CS, sumsq_CS)
          var (closest_DS, mindist) = dist_to_DS.minBy(_._2)

          if (mindist < base){
            del_cluster :+= cluster
            n1(closest_DS) += n_CS(cluster)
            sum1(closest_DS) += sum_CS(cluster)
            sumsq1(closest_DS) += sumsq_CS(cluster)

            for ((row, label) <- CS){
              if (label == cluster){
                res += (vec_to_id(row) -> closest_DS)
                ds += (row -> closest_DS)
              }
            }

          }

        }
        val CS_filtered = CS.retain((k,v) => !del_cluster.contains(v))
        CS = CS_filtered
        for ((vec, label) <- CS_filtered){
          res.remove(vec_to_id(vec))
          res += (vec_to_id(vec) -> -1)
        }
        for (vec <- RS){
          res.remove(vec_to_id(vec))
          res += (vec_to_id(vec) -> -1)
        }




      println("Step 13 Done")
      }

      n_DS = 0
      for ((label, count) <- n1.toList) {
        n_DS += count
      }
      var CS_swap = CS.map(_.swap)

//      println("Round %d: ".format(round) + ds.size.toString + "," + CS_swap.keys.toSet.size.toString + "," + CS.size.toString + "," + RS.size.toString + '\n')
//      println("Round %d: ".format(round) + n_DS.toString + "," + CS_swap.keys.toSet.size.toString + "," + CS.size.toString + "," + RS_count.toString + '\n')
      bw.write("Round %d: ".format(round) + ds.size.toString + "," + CS_swap.keys.toSet.size.toString + "," + CS.size.toString + "," + RS.size.toString + '\n')

    }


    bw.write("\n")
    bw.write("The clustering results:\n")
    for (idx <- 0 to res.keys.size-1) {
      bw.write(idx.toString + "," + res(idx).toString + "\n")
    }

    val duration = ((System.nanoTime - t1) / 1e9d).toString
    println("Duration: ".concat(duration))
    bw.close()

  }
}