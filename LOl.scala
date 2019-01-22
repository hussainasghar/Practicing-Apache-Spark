package scala_basic

object LOl {
  def main(args: Array[String]): Unit = {

    print(rational(4,3))


    def rational(a:Int,b:Int):Double={
      print(a+"/"+b+"\n")
      a/b
    }

    def gcd(a:Int,b:Int):Double=  if (b==0) 1 else gcd(b,a%b)



    def listInMethod(names: List[String]): Unit ={

      for(name <- names) print(name+" ")
    }


    try{
      val a = 2/0
    }
    catch {
      case ex:IllegalArgumentException=>  new IllegalArgumentException("Can't divied by 0")
    }

    def calculator(a:Int,op:Char,b:Int):Double={

      op match {
        case '+'=> a+b
        case '-' => a-b
        case '*' => a*b
        case '/' => a/b
        case _ => a%b //Default
      }
    }



  }
}
