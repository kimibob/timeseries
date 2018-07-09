package com.zq.timeseries

import java.sql.Timestamp
import java.time.{ LocalDateTime, ZoneId, ZonedDateTime }

import com.cloudera.sparkts._
import com.cloudera.sparkts.stats.TimeSeriesStatisticalTests

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.sql.{ DataFrame, Row, SQLContext }
import org.apache.spark.sql.types._
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{ Vectors, Vector }

/**
 * An example exhibiting the use of TimeSeriesRDD for loading, cleaning, and filtering stock ticker
 * data.
 */
object Stocks {
  /**
   * Creates a Spark DataFrame of (timestamp, symbol, price) from a tab-separated file of stock
   * ticker data.
   */
  def loadObservations(sqlContext: SQLContext, path: String): DataFrame = {
    val rowRdd = sqlContext.sparkContext.textFile(path).map { line =>
      val tokens = line.split('\t')
      val dt = ZonedDateTime.of(tokens(0).toInt, tokens(1).toInt, tokens(2).toInt, 0, 0, 0, 0,
        ZoneId.systemDefault())
      val symbol = tokens(3)
      val price = tokens(5).toDouble
      Row(Timestamp.from(dt.toInstant), symbol, price)
    }
    val fields = Seq(
      StructField("timestamp", TimestampType, true),
      StructField("symbol", StringType, true),
      StructField("price", DoubleType, true))
    val schema = StructType(fields)
    //形成dataframe格式：timestamp，symbol，price
    sqlContext.createDataFrame(rowRdd, schema)
  }

  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    //设置环境
    System.setProperty("hadoop.home.dir", "F:\\hadoop-2.6.5\\")
    val conf = new SparkConf().setAppName("Spark-TS Ticker Example").setMaster("local")
    conf.set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val tickerObs = loadObservations(sqlContext, "data/ticker2.tsv")

    // Create an daily DateTimeIndex over August and September 2015
    val zone = ZoneId.systemDefault()
    val dtIndex = DateTimeIndex.uniformFromInterval(
      ZonedDateTime.of(LocalDateTime.parse("2015-08-03T00:00:00"), zone),
      ZonedDateTime.of(LocalDateTime.parse("2015-08-06T00:00:00"), zone),
      new BusinessDayFrequency(1))

    // Align the ticker data on the DateTimeIndex to create a TimeSeriesRDD
    //其中是有缺失值的
    val tickerTsrdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, tickerObs,
      "timestamp", "symbol", "price")
    tickerTsrdd.foreach(println)
    println("-----------------------------------------------------")
    // Cache it in memory
    tickerTsrdd.cache()

    // Count the number of series (number of symbols)
    //key合并后的总个数
    //println(tickerTsrdd.count())

    // Impute missing values using linear interpolation
    //填充缺失值
    val filled = tickerTsrdd.fill("linear")
    tickerTsrdd.foreach(println)
    println("----------------------------------------------------")
    // Compute return rates(相当于相邻两个数的增长率)
    val returnRates = filled.returnRates()
    //returnRates.foreach(println)
    println("----------------------------------------------------")

    // Compute Durbin-Watson stats for each series
    //残差(估计值和真实值之差)分析，检验残差中是否存在自相关。
    val dwStats = returnRates.mapValues(TimeSeriesStatisticalTests.dwtest)
    dwStats.foreach(println)
    //println(dwStats.map(_.swap).min)
    //println(dwStats.map(_.swap).max)

    val timeSeriesModel = new TimeSeriesModel(4, "outputtablename")
    var forecastValue: RDD[(String, Vector)] = sc.parallelize(Seq(("", Vectors.dense(1))))
    //选择模型
    val modelName="arima"
    modelName match {
      case "arima" => {
        //创建和训练arima模型
        val (forecast, coefficients) = timeSeriesModel.arimaModelTrain(filled)
        //Arima模型评估参数的保存
        forecastValue = forecast
        timeSeriesModel.arimaModelEvaluationSave(coefficients, forecast, sqlContext)
      }
      case "holtwinters" => {
        //创建和训练HoltWinters模型(季节性模型)
        val (forecast, sse) = timeSeriesModel.holtWintersModelTrain(filled, 12, "Multiplicative")
        //HoltWinters模型评估参数的保存
        forecastValue = forecast
        timeSeriesModel.holtWintersModelEvaluationSave(sse, forecast, sqlContext)
      }
      case _ => throw new UnsupportedOperationException("Currently only supports 'ariam' and 'holtwinters")
    }

    //合并实际值和预测值，并加上日期,形成dataframe(Date,Data)，并保存
    //timeSeriesModel.actualForcastDateSaveInHive(filled, forecastValue, predictedN, startTime, endTime, sc, hiveColumnName, "0.0", sqlContext)
  }
}