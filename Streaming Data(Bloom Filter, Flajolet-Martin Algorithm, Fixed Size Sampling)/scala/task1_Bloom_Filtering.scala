import java.io._

object task1 {

  def main(args: Array[String]): Unit = {
    val r1 = scala.util.Random
    r1.setSeed(553)
    val t1 = System.nanoTime

    // get argument
    val input_filename = args(0)
    val stream_size = args(1).toInt
    val num_of_asks = args(2).toInt
    val output_filename = args(3)
    val prime_list = List(17393, 17401, 17417, 17419, 17431, 17443, 17449, 17467, 17471, 17477, 17483, 17489, 17491, 17497,
      17509, 17519, 17539, 17551, 17569, 17573, 17579, 17581, 17597, 17599, 17609, 17623, 17627, 17657,
      17659, 17669, 17681, 17683, 17707, 17713, 17729, 17737, 17747, 17749, 17761, 17783, 17789, 17791,
      17807, 17827, 17837, 17839, 17851, 17863, 17881, 17891, 17903, 17909, 17911, 17921, 17923, 17929,
      17939, 17957, 17959, 17971, 17977, 17981, 17987, 17989, 18013, 18041, 18043, 18047, 18049, 18059,
      18061, 18077, 18089, 18097, 18119, 18121, 18127, 18131, 18133, 18143, 18149, 18169, 18181, 18191,
      18199, 18211, 18217, 18223, 18229, 18233, 18251, 18253, 18257, 18269, 18287, 18289, 18301, 18307,
      18311, 18313, 18329, 18341, 18353, 18367, 18371, 18379, 18397, 18401, 18413, 18427, 18433, 18439,
      18443, 18451, 18457, 18461, 18481, 18493, 18503, 18517, 18521, 18523, 18539, 18541, 18553, 18583,
      18587, 18593, 18617, 18637, 18661, 18671, 18679, 18691, 18701, 18713, 18719, 18731, 18743, 18749,
      18757, 18773, 18787, 18793, 18797, 18803, 18839, 18859, 18869, 18899, 18911, 18913, 18917, 18919,
      18947, 18959, 18973, 18979, 19001, 19009, 19013, 19031, 19037, 19051, 19069, 19073, 19079, 19081,
      19087, 19121, 19139, 19141, 19157, 19163, 19181, 19183, 19207, 19211, 19213, 19219, 19231, 19237,
      19249, 19259, 19267, 19273, 19289, 19301, 19309, 19319, 19333, 19373, 19379, 19381, 19387, 19391,
      19403, 19417, 19421, 19423, 19427, 19429, 19433, 19441, 19447, 19457, 19463, 19469, 19471, 19477,
      19483, 19489, 19501, 19507, 19531, 19541, 19543, 19553, 19559, 19571, 19577, 19583, 19597, 19603,
      19609, 19661, 19681, 19687, 19697, 19699, 19709, 19717, 19727, 19739, 19751, 19753, 19759, 19763,
      19777, 19793, 19801, 19813, 19819, 19841, 19843, 19853, 19861, 19867, 19889, 19891, 19913, 19919,
      19927, 19937, 19949, 19961, 19963, 19973, 19979, 19991, 19993, 19997, 20011, 20021, 20023, 20029,
      20047, 20051, 20063, 20071, 20089, 20101, 20107, 20113, 20117, 20123, 20129, 20143, 20147, 20149,
      20161, 20173, 20177, 20183, 20201, 20219, 20231, 20233, 20249, 20261, 20269, 20287, 20297, 20323,
      20327, 20333, 20341, 20347, 20353, 20357)


    val len_filter = 69997
    val num_hash_ftn = 2
    val m = len_filter
    var bloom_filter = (for (i <- 0 to m - 1) yield 0).toList
    var previous_user_set : Set[String] = Set()
    val hash_function_list = for (i <- 1 to num_hash_ftn) yield {
      (x: Long) => ((y: Int) => (prime_list(y%283) * x + prime_list((2 * y + 3)%281)) % prime_list((3 * y + 1)%239)) (i)
    }

    def myhashs(user: String): List[Int] = {
      var result: List[Int] = List()
      var num = user.hashCode.abs
//      println(hash_function_list(0)(num))
      for (f <- hash_function_list) {
        result = result ++ List(f(num).toInt)
      }
      return result
    }

    def myftn(stream_users: List[String]): Double = {
      var FP : Double = 0
      var Actual_Negative : Double = 0
      for ((user, idx) <- stream_users.zipWithIndex) {
        var hash_vals = myhashs(user)
//        println(hash_vals)
        if (previous_user_set.contains(user) == false) {
          Actual_Negative += 1.0
          var not_yet = false
          for (value <- hash_vals) {
            if (bloom_filter(value) == 0) {
              not_yet = true
            }
          }
          if (not_yet == false) {
            FP = FP + 1.0
          }
        }
//        println(hash_vals)
        for (value <- hash_vals) {
          bloom_filter = bloom_filter.updated(value, 1)
        }
//        println(bloom_filter.sum)


      }
      return FP / Actual_Negative

    }


    var FPR : List[Double] = List()
    val box = new Blackbox
    for (i <- 1 to num_of_asks) {
      var stream_users = box.ask(input_filename, stream_size).toList
      var fpr = myftn(stream_users)
      FPR = FPR++List(fpr)
      previous_user_set = (previous_user_set.toList ++ stream_users).toSet
    }
    val file = new File(output_filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("Time,FPR\n")
    for ((fpr, idx) <- FPR.zipWithIndex){
      bw.write(idx.toString)
      bw.write(",")
      bw.write(fpr.toString)
      bw.write("\n")
    }
    bw.close()

    val duration = ((System.nanoTime - t1) / 1e9d).toString
    println("Duration: ".concat(duration))






  }
}

