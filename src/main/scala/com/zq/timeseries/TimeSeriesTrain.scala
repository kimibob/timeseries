package com.zq.timeseries

import java.sql.Timestamp
import java.time.{ZoneId, ZonedDateTime}

import com.cloudera.sparkts._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.DenseVector
import scala.util.Try

/**
 * 时间序列模型time-series的建立
 */
object TimeSeriesTrain {

  /**
   * 把数据中的“time”列转换成固定时间格式：ZonedDateTime（such as 2007-12-03T10:15:30+01:00 Europe/Paris.）
   * @param timeDataKeyDf
   * @param sqlContext
   * @param hiveColumnName
   * @return zonedDateDataDf
   */
  def timeChangeToDate(timeDataKeyDf:DataFrame,sqlContext: SQLContext,columnName:List[String],startTime:String,sc:SparkContext): DataFrame ={
    var rowRDD:RDD[Row]=sc.parallelize(Seq(Row(""),Row("")))
    //空值填充
    val newDf = timeDataKeyDf.na.fill("0",Seq("bytes"))
    //具体到月份
    if(startTime.length==6){
      rowRDD=newDf.rdd.map{row=>
        Try(
          row match{
            case Row(time,key,data)=>{
              val dt = ZonedDateTime.of(time.toString.substring(0,4).toInt,time.toString.substring(4).toInt,1,0,0,0,0,ZoneId.systemDefault())
              Row(Timestamp.from(dt.toInstant), key.toString, data.toString.toDouble)
            }
          }
        )
      }.filter(_.isSuccess).map(_.get)
    }else if(startTime.length==8){
      //具体到日
      rowRDD=newDf.rdd.map{row=>
        Try(
          row match{
            case Row(time,key,data)=>{
              val dt = ZonedDateTime.of(time.toString.substring(0,4).toInt,time.toString.substring(4,6).toInt,time.toString.substring(6).toInt,0,0,0,0,ZoneId.systemDefault())
              Row(Timestamp.from(dt.toInstant), key.toString, data.toString.toDouble)
            }
          }
        )
      }.filter(_.isSuccess).map(_.get)
    }
    //根据模式字符串生成模式，转化成dataframe格式
    val field=Seq(
      StructField(columnName(0), TimestampType, true),
      StructField(columnName(0)+"Key", StringType, true),
      StructField(columnName(1), DoubleType, true)
    )
    val schema=StructType(field)
    val zonedDateDataDf=sqlContext.createDataFrame(rowRDD,schema)
    return zonedDateDataDf
  }


  /**
   * 总方法调用
   * @param args
   */
  def main(args: Array[String]) {
    
    /*****参数输入*****/
    val inputfile_path = args(0)
    val outputDir = args(1)
    val conf = new SparkConf().setAppName("Spark TimeSeries APP wy")
    val runflag = args(2)
    val startTime = args(3)
    val endTime= args(4)
    val predictedN= args(5).toInt
    /*****环境设置*****/
    //shield the unnecessary log in terminal
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //set the environment
//    System.setProperty("hadoop.home.dir", "E:\\bigdata-sourcecode\\hadoop_src\\hadoop-2.7.0\\")
//    val conf = new SparkConf().setAppName("timeSeries.local.TimeSeriesTrain").setMaster("local[2]")
//    val outputDir = "data/resDir"
//    val inputfile_path = "data/dataflow.csv"
//    val startTime="20180301"
//    val endTime="20180731"
//    val predictedN=184
//    val runflag = "localtime3"
    
    
    val sc = new SparkContext(conf)
    val sqlContext=new SQLContext(sc)
    /*
     * 从Spark2.0以上版本开始，Spark使用全新的SparkSession接口替代Spark1.6中的SQLContext及HiveContext接口来实现其对数据加载、转换、处理等功能。
     * SparkSession实现了SQLContext及HiveContext所有功能。
     */
    //val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    
    /*****参数设置*****/
    //hive中的数据库名字.数据表名
    val databaseTableName="time_series.jxt_electric_month"
    //选择模型(holtwinters或者是arima)
    val modelName="arima"
    //数据表中要处理的time和data列名（输入表中用于训练的列名,必须前面是时间，后面是data）
    val columnName=List("time","data")

    //存放的表名字
    val outputTableName="timeseries_outputdate"

    //只有holtWinters才有的参数
    //季节性参数（12或者4）
    val period=60
    //holtWinters选择模型：additive（加法模型）、Multiplicative（乘法模型）
    val holtWintersModelType="additive"

    /*****读取数据和创建训练数据*****/

    //val csvfile = sparkSession.read.csv("data/dataflow20180710.csv")
//    val csvfile = sqlContext.read.csv(inputfile_path)
//                  .withColumnRenamed("_c0", "date")
//                  .withColumnRenamed("_c1", "cellid")
//                  .withColumnRenamed("_c2", "bytes")
    
//    val csvfile = sqlContext.read
//    .format("com.databricks.spark.csv")
//    //.option("mode", "DROPMALFORMED")
//    .load("data/dataflow.csv"); 
    
    val csvfile = sqlContext.read.format("com.databricks.spark.csv").load(inputfile_path); 
    
    csvfile.printSchema()
    val zonedDateDataDf=timeChangeToDate(csvfile,sqlContext,columnName,startTime,sc)
    //println(zonedDateDataDf.show(false))

    /**
     * 创建数据中时间的跨度（Create an daily DateTimeIndex）:开始日期+结束日期+递增数
     * 日期的格式要与数据库中time数据的格式一样
     */
    //参数初始化
    val zone = ZoneId.systemDefault()
    var dtIndex:UniformDateTimeIndex=DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(2018, 2, 4, 0, 0, 0, 0, zone),
      ZonedDateTime.of(2018, 6, 30, 0, 0, 0, 0, zone),
      new DayFrequency(1))

    //具体到月份
    if(startTime.length==6) {
      dtIndex = DateTimeIndex.uniformFromInterval(
        ZonedDateTime.of(startTime.substring(0, 4).toInt, startTime.substring(4).toInt, 1, 0, 0, 0, 0, zone),
        ZonedDateTime.of(endTime.substring(0, 4).toInt, endTime.substring(4).toInt, 1, 0, 0, 0, 0, zone),
        new MonthFrequency(1))
    }else if(startTime.length==8){
      //具体到日,则把dtIndex覆盖了
      dtIndex = DateTimeIndex.uniformFromInterval(
        ZonedDateTime.of(startTime.substring(0,4).toInt,startTime.substring(4,6).toInt,startTime.substring(6).toInt,0,0,0,0,zone),
        ZonedDateTime.of(endTime.substring(0,4).toInt,endTime.substring(4,6).toInt,endTime.substring(6).toInt,0,0,0,0,zone),
        new DayFrequency(1))
    }
    //System.exit(0)
    //创建训练数据TimeSeriesRDD(key,DenseVector(series))

    val trainTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, zonedDateDataDf,
      columnName(0), columnName(0)+"Key", columnName(1))
    //填充缺失值
    val trainTsrddnotnull = trainTsrdd.mapSeries { values =>
      val result = values.copy.toArray
      var i = result.size - 1
      while (i >= 0) {
          if(result(i).isNaN) result(i) = 0.0
          i -= 1
      }
      new DenseVector(result)
    }
    //插值法填充空值
    //val filledTrainTsrdd = trainTsrdd.fill("linear")
    trainTsrddnotnull.cache()
    //trainTsrddnotnull.foreach(println)
    /*****建立Modle对象*****/
    val timeSeriesModel=new TimeSeriesModel(predictedN,outputTableName)
    var forecastValue:RDD[(String,Vector)]=sc.parallelize(Seq(("",Vectors.dense(1))))
    //选择模型
    modelName match{
      case "arima"=>{
        //创建和训练arima模型
        val forecast=timeSeriesModel.arimaModelTrain(trainTsrddnotnull)
        //Arima模型评估参数的保存
        forecastValue=forecast
        //timeSeriesModel.arimaModelEvaluationSave(coefficients,forecast,sqlContext)
      }
      case "holtwinters"=>{
        //创建和训练HoltWinters模型(季节性模型)
        val (forecast,sse) =timeSeriesModel.holtWintersModelTrain(trainTsrddnotnull,period,holtWintersModelType)
        //HoltWinters模型评估参数的保存
        forecastValue=forecast
        //timeSeriesModel.holtWintersModelEvaluationSave(sse,forecast,sqlContext)
      }
      case _=>throw new UnsupportedOperationException("Currently only supports 'ariam' and 'holtwinters")
    }
    forecastValue.cache();
    //合并实际值和预测值，并加上日期,形成dataframe(Date,Data)，并保存
    println("save file...")
    timeSeriesModel.actualForcastDateSave(trainTsrddnotnull,forecastValue,predictedN,startTime,endTime,sc,columnName,"dataflow",sqlContext,outputDir,runflag)
  }
}