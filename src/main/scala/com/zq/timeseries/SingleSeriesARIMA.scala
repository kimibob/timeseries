package com.zq.timeseries

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.mllib.linalg.Vectors

import com.cloudera.sparkts.models.ARIMA
import java.math.BigDecimal

/**
 * An example showcasing the use of ARIMA in a non-distributed context.
 */
object SingleSeriesARIMA {
  def main(args: Array[String]): Unit = {
    // The dataset is sampled from an ARIMA(1, 0, 1) model generated in R.
    val lines = scala.io.Source.fromFile("data/R_ARIMA_DataSet.csv").getLines()
    val ts = Vectors.dense(lines.map(_.toDouble).toArray)
    val arimaModel = ARIMA.fitModel(1, 1, 1, ts)
    //参数输出：
    //println("coefficients: " + arimaModel.coefficients.mkString(","))
    val forecast = arimaModel.forecast(ts, 50)
    //预测出后五个的值。
    //println("arimaModel forecast of next 5 observations: ")
    forecast.toArray.map(a => new BigDecimal(a.toDouble).toPlainString()).foreach(println)

    /**
     * HoltWinters
     */
    val holtWintersModel = HoltWinters.fitModel(ts,12,"Multiplicative")
    //构成N个预测值数组
    val predictedArrayBuffer=new ArrayBuffer[Double]()
    var i=0
    while(i<100){
      predictedArrayBuffer+=i
      i=i+1
    }
    val predictedVectors=Vectors.dense(predictedArrayBuffer.toArray)
    val forecast2 = holtWintersModel.forecast(ts,predictedVectors)
    //预测出后五个的值。
    //println("HoltWinters forecast of next 5 observations: ")
    //forecast2.toArray.map(a => new BigDecimal(a.toDouble).toPlainString()).foreach(println)
  }
}