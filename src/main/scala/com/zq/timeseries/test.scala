package com.zq.timeseries

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf

object test {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\")
    val conf = new SparkConf().setAppName("timeSeries.local.TimeSeriesTrain").setMaster("local[2]")
     /*****环境设置*****/
    //shield the unnecessary log in terminal
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //set the environment
    //System.setProperty("hadoop.home.dir", "F:\\hadoop-2.6.5\\")
//    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\")
//    val conf = new SparkConf().setAppName("timeSeries.local.TimeSeriesTrain").setMaster("local[2]")
    
    val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    /*
     * 从Spark2.0以上版本开始，Spark使用全新的SparkSession接口替代Spark1.6中的SQLContext及HiveContext接口来实现其对数据加载、转换、处理等功能。
     * SparkSession实现了SQLContext及HiveContext所有功能。
     */
    
    print("a,b,c,".subSequence(0, "a,b,c,".length()-1))
  }
}