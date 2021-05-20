import java.io.{BufferedWriter, File, FileWriter}
import scala.io.Source

class Blackbox{
  private val r1 = scala.util.Random
  r1.setSeed(553)
  def ask(filename: String, num: Int): Array[String] = {
    val input_file_path = filename

    val lines = Source.fromFile(input_file_path).getLines().toArray
    val stream = new Array[String](num)

    for (i <- 0 to num-1){
      stream(i) = lines(r1.nextInt(lines.length))
    }
    return stream
  }
}

object task3 {

  def main(args: Array[String]): Unit = {
    val r1 = scala.util.Random
    r1.setSeed(553)

    val t1 = System.nanoTime

    // get argument
    val input_filename = args(0)
    val stream_size = args(1).toInt
    val num_of_asks = args(2).toInt
    val output_filename = args(3)

    var mem : List[String] = List()
    val box = new Blackbox
    var count = 0
    val random_object = scala.util.Random
    val file = new File(output_filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("seqnum,0_id,20_id,40_id,80_id\n")

    var a : Float = 0
    var b : Int = 0

    for ( k <- 0 to num_of_asks-1){
      var stream_users : List[String] = box.ask(input_filename, stream_size).toList
      if (stream_size * ( k + 1) <= 100) {
        for ((user, idx) <- stream_users.zipWithIndex) {
          mem = mem ++ List(user)
          count = count + 1
        }
      }

      if (stream_size * (k + 1) > 100){
        var idx = 0
        while (mem.size <100){
          mem = mem ++ List(stream_users(idx))
          idx  = idx + 1
        }

        while (idx < stream_size){
          count = count + 1

          a = random_object.nextFloat()

          if (a <= 100/(count.toFloat)){
            b = random_object.nextInt(100)
            mem = mem.updated(b, stream_users(idx))
          }
          idx = idx + 1
        }
      }
      var seqnum = stream_size * (k + 1)
      var id_0 = mem(0)
      var id_20 = mem(20)
      var id_40 = mem(40)
      var id_60 = mem(60)
      var id_80 = mem(80)
      bw.write(seqnum.toString)
      bw.write(",")
      bw.write(id_0)
      bw.write(",")
      bw.write(id_20)
      bw.write(",")
      bw.write(id_40)
      bw.write(",")
      bw.write(id_60)
      bw.write(",")
      bw.write(id_80)
      bw.write("\n")

    }
    bw.close()

    val duration = ((System.nanoTime - t1) / 1e9d).toString
    println("Duration: ".concat(duration))






  }
}

