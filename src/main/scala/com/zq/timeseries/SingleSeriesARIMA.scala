package com.zq.timeseries

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.linalg.Vectors

import com.cloudera.sparkts.models.ARIMA

/**
 * An example showcasing the use of ARIMA in a non-distributed context.
 */
object SingleSeriesARIMA {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.
    val lines = scala.io.Source.fromFile("data/R_ARIMA_DataSet1.csv").getLines()
    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
    val arimaModel = ARIMA.fitModel(1, 0, 1, ts)
    //参数输出：
    println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 5)
    //预测出后五个的值。
    println("forecast of next 5 observations: " + forecast.toArray.mkString(","))

    /**
     * HoltWinters
     */
    val holtWintersModel = HoltWinters.fitModel(ts,12,"Multiplicative")
    //构成N个预测值数组
    val predictedArrayBuffer=new ArrayBuffer[Double]()
    var i=0
    while(i<31){
      predictedArrayBuffer+=i
      i=i+1
    }
    val predictedVectors=Vectors.dense(predictedArrayBuffer.toArray)
    val forecast2 = holtWintersModel.forecast(ts,predictedVectors)
    //预测出后五个的值。
    println("forecast of next 5 observations: " + forecast2.toArray.mkString(","))
  }
}