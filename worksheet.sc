

def findKElement[A](i: Int, list: List[A]): A = (i, list) match {
  case (0, h :: _) => h
  case (n, _ :: tail) => findKElement(n - 1, tail)
  case (_, Nil) => throw new Exception

}

def size[A](list: List[A]): Int = list match {
  case Nil => 0
  case (_ :: tail) => 1 + size(tail)
}

def isPalindrome[A](list: List[A]): Boolean = (list == list.reverse)
isPalindrome(list)

def distinct[A](list: List[A]): List[A] = list match {
  case Nil => Nil
  case (i :: tail) => i :: distinct(tail.dropWhile(_ == i))
}


def slice[A](start: Int, finish: Int, list: List[A]): List[A] = {
  def findStart[A](i: Int, list: List[A]): List[A] = (i, list) match {
    case (0, h :: tail) => (h :: tail)
    case (n, _ :: tail) => findStart(n - 1, tail)
    case (_, Nil) => throw new Exception
  }

  def findFinish[A](i: Int, list: List[A]): List[A] = (i, list) match {
    case (0, h :: _) => h :: Nil
    case (n, h :: tail) => h :: findFinish(n - 1, tail)
    case (_, Nil) => throw new Exception
  }

  findFinish((finish - start), findStart(start, list))
}




