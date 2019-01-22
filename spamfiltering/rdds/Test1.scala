package spamfiltering.rdds

/**
  * Created by hussain on 1/5/18.
  */
object Test1 {
  def main(args: Array[String]): Unit = {


    val data = "This is a test string. Now we or and a in the room on not saturday email as for now , * : < > America ' - : 0 1 ; : , @"

    val stopWords= Seq("the", "a", "", "in", "on", "at", "as", "not", "for","to","will","its",
      "to","was","were","he","has","with","as","from","by")
    // Ignore all useless characters
    val ignoredChars = Seq(",", ":", ";", "/", "<", ">", "." , "(", ")", "?", "-", "\",","!")

    val cleaning = data.split(" ").filter(x => !stopWords.contains(x) && !ignoredChars.contains(x)).foreach(println)




  }
}