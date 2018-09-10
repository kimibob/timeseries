package com.zq.timeseries

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode

object TimeGompertz {
  
  def getGompertz(k: Double, a: Double, b: Double, t: Double) :Double= {
    k*math.pow(a, math.pow(b, t));
  }

  
  def main(args: Array[String]): Unit = {
     /*****参数输入*****/
    val inputfile_path = args(0)
    val outputDir = args(1)
    val conf = new SparkConf().setAppName("Spark TimeGompertz APP wy")
    //val outputDir = "data/resDir"
    //val inputfile_path = "data/dataflow2.csv"
    
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
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    
    val group_num = 3;
    val data_length = 147;
    val n = data_length/group_num;
    val predict_num = 184;
    
    val rowsRDD = sparkSession.sparkContext.textFile(inputfile_path)
    val a = rowsRDD.map(row => row.split(",")).map(col => (col(1),(col(0),col(2))))
            .groupByKey().mapValues(_.toList.sortBy(_._1)).mapValues(list => (list.map(_._2.toDouble), list.map(_._2.toDouble).map(math.log(_))))
            .mapValues(list => (list._2.slice(0, n), list._2.slice(n, 2*n), list._2.slice(2*n, 3*n), list._1))
    //a.foreach(println)
    val b = a.mapValues(listgroup => (listgroup._4, (listgroup._1.sum, listgroup._2.sum, listgroup._3.sum)))

    //(key, (sum_ln1, sum_ln2, sum_ln3, b, train_datalist) )
    val c = b.mapValues(v => (v._2._1,v._2._2,v._2._3, math.pow((v._2._3 - v._2._2)/(v._2._2 - v._2._1),1.0/n), v._1 )   )
    
    //d => (key, (sum_ln1, sum_ln2, sum_ln3, b, ln_a, train_datalist) )
    val d = c.mapValues{
      v => {
        val ln_a = (v._4 - 1)*(v._2 - v._1)/(v._4 * math.pow((math.pow(v._4, n)-1),2))
        (v._1, v._2, v._3, v._4, ln_a, v._5)
      }
    }

    //e => (key, (sum_ln1, sum_ln2, sum_ln3, b, a, k, train_datalist) )
    val e = d.mapValues{
      v => {
        val ln_k = (v._1 - (math.pow(v._4, n)-1) * v._4 * v._5/(v._4 - 1))/n
        (v._1,v._2,v._3,v._4,math.exp(v._5),math.exp(ln_k),v._6)
      }
    }

    val f = e.mapValues{
      v => {
        val data = v._7.toBuffer
        for (t <- data_length+1 to data_length+predict_num) {
          data += getGompertz(v._6, v._5, v._4, t)
        }
        data.map(_.formatted("%.2f")).mkString(",")
      }
    }.map(v => v._1+","+v._2)
    import sparkSession.implicits._
    val resDF = f.toDF()
    val saveOptions = Map("header" -> "false", "path" -> outputDir)    
    resDF//.coalesce(1)
    .write.format("text").mode(SaveMode.Overwrite).options(saveOptions).save()
  }
}