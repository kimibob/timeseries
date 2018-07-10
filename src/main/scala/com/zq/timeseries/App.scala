package com.zq.timeseries

import java.time.ZoneId
import java.time.ZonedDateTime
import java.text.SimpleDateFormat
import java.sql.Timestamp

/**
 * @author ${user.name}
 */
object App {

  def main(args: Array[String]) {
    
    val zone = ZoneId.systemDefault()
    println(zone)
    println(ZonedDateTime.of(2018, 2, 4, 0, 0, 0, 0, zone))
    val fm = new SimpleDateFormat("yyyyMMdd")
    val tm = "20180204"
    val dt = fm.parse(tm);
    println(dt.getTime())
    println("tt "+Timestamp.from(dt.toInstant))
    val dt1 = ZonedDateTime.of(2018, 2, 4, 0, 0, 0, 0, zone)
    println("dt1 "+Timestamp.from(dt1.toInstant))

  }

}