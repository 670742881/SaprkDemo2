package com.spark.realtime

/**
  * @created by imp ON 2019/2/25
  */
//* List学习，两个冒号(:)和三个冒号(:)的使用
//*/
object ListTest {

  /**
    * :: 该方法被称为cons，意为构造，向队列的头部追加数据，创造新的列表。用法为 x::list,其中x为加入到头部的元素，无论x是列表与否，它都只将成为新生成列表的第一个元素，也就是说新生成的列表长度为list的长度＋1(btw,x::list等价于list.::(x))
    *
    * :+和+: 两者的区别在于:+方法用于在尾部追加元素，+:方法用于在头部追加元素，和::很类似，但是::可以用于pattern match ，而+:则不行. 关于+:和:+,只要记住冒号永远靠近集合类型就OK了。
    *
    * ++ 该方法用于连接两个集合，list1++list2
    *
    * ::: 该方法只能用于连接两个List类型的集合
    *
    *
    * @param args
    */

  def main(args: Array[String]): Unit = {
    val one = List(1,2,3,4,5)
    val two = List(6,7,8,9,10)

    // List ::: 方法实现叠加功能
    val three = one ::: two
    // 此时 three的值是：List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(three)

    // List :: 方法是实现把新元素添加到已有List的最前端
    val four = 1 :: three
    // four的值是：List(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    println(four)
  }

}
