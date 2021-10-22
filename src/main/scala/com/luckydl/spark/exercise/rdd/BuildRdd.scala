package com.luckydl.spark.exercise.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author luckyDL
 * @date create at 2021/10/22 21:41
 */
object BuildRdd {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Build rdd")
    val sc = new SparkContext(conf)
    fromMemory(sc)
    //    fromFile()
    sc.stop()
  }

  /**
   * 在内存中创建RDD的例子
   */
  def fromMemory(sc: SparkContext): Array[Unit] = {
    sc.makeRDD(Array(1, 2, 3, 4), 2).map(println).collect()
  }

  def fromFile(sc: SparkContext): Unit = {
    val file = sc.textFile("data/1.txt", 2)
    file.saveAsTextFile("output")
  }
}
