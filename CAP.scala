package rdd

/**
  * Created by hussain on 10/15/17.
  */
object CAP {

  def main(args: Array[String]) {


    def mySumMethod(a: Int, b: Int): Int = a + b

    //> mySumMethod: (a: Int, b: Int)Int

    val mySumFunction1: (Int, Int) => Int = _ + _
    //> mySumFunction1: (Int, Int) => Int = <function2>

    val mySumFunction2 = (a: Int, b: Int) => a + b
    //> mySumFunction2: (Int, Int) => Int = <function2>

    def sumSquare(l: List[Int]): Int = {
      var accum = 0
      for (i <- 0 to l.size - 1) {
        accum += l(i) * l(i)
      }
      return accum
    }

   // Functional style with an anonymous function
    println(sumSquare(List(1, 2, 3, 4)))


    def sumSquaree(l: List[Int]): Int = l.map(x => x * x).sum

    def sumSquareee(l: List[Int]) = l.map(x => x * x).sum

    val square = (x: Int) => x * x
    def sumSquareeee(l: List[Int]) = l.map(x => square(x)).sum

    def sumSquareeeee(l: List[Int]) = l.map(square(_)).sum

    def sumSquares(l: List[Int]) = l.map(square).sum

    println( (1 to 4).map(square).sum )

    //Recursion ,pattern matching
    def sum(l: List[Int]): Int = l match {
      case Nil => 0
      case x :: xs => x + sum(xs)
    }
  //Other technique
    def sum1(l: List[Int]): Int = {
      if (l.isEmpty) 0
      else l.head + sum1(l.tail)
    }

    def sums(l: List[Int]): Int = {
     // @tailrec
      def sumAccumulator(l: List[Int], accum: Int): Int = {
        l match {
          case Nil => accum
          case x :: xs => sumAccumulator(xs, accum + x)
        }
      }
      sumAccumulator(l, 0)
    }

    val array = Array(1, 2, 3, 4, 5, 6)
    val tuple = ('a', 1, 'b', 2, 'c')
    val tuple2 = (('a', 1), ('b', 2))
    println(array(0))
    println(tuple._1)
    println(tuple2._1)

    // val splitTabs = sc.textFile("/SEIS736/tweets").map(x => x.split(“\\t”))

  //   val badTweets = sc.accumulator(0)

  //   splitTabs.map(x => if (x.size < 2) badTweets +=1).count


  }
}
