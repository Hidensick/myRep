import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import scala.collection.mutable


object MilanTelecomData extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  Logger.getLogger("netty").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
    .setAppName("Milan_pollution")
    .setMaster("local[*]")
    .set("spark.driver.allowMultipleContexts", "true")

  val sc = new SparkContext(sparkConf)

  val telecomData = sc.textFile(".\\dataset\\Milano\\teleData\\*\\*.txt")
    .flatMap {
      _.split("\t", -1) match {
        case Array(square_id, time_interval, country_code, smsIn_act, smsOut_act, callIn_act, callOut_act, intAct) => {
          val date: DateTime = new DateTime(time_interval.toLong)
          if (date.getDayOfWeek != 6 && date.getDayOfWeek != 7
            && (date.getMonthOfYear == 11 && date.getDayOfMonth != 1)
            && (date.getMonthOfYear == 12 && (date.getDayOfMonth != 25 || date.getDayOfMonth != 26))
            && (((date.getHourOfDay > 9) && (date.getHourOfDay < 13)) || ((date.getHourOfDay > 15) && (date.getHourOfDay < 17)))) {
            var act = 0d
            if (!smsIn_act.isEmpty)
              act += smsIn_act.toDouble
            if (!smsOut_act.isEmpty)
              act += smsOut_act.toDouble
            if (!callIn_act.isEmpty)
              act += callIn_act.toDouble
            if (!callOut_act.isEmpty)
              act += callOut_act.toDouble
            if (!intAct.isEmpty)
              act += intAct.toDouble
            Some(square_id, act)
          } else None
        }
        case _ => None
      }
    }.aggregateByKey(0d)(
    (acc, act) => acc + act,
    (acc1, acc2) => acc1 + acc2
  )

  val avg = telecomData.values.sum / telecomData.values.count()

  val area = telecomData.flatMap {
    case (square_id, act) => if (act < avg)
      Some(square_id.toLong, "residential")
    else
      Some(square_id.toLong, "industrial")
  }

  val pollution_leg = sc.textFile(".\\dataset\\Milano\\pollution-legend-mi.csv")
    .flatMap {
      _.split(",", -1) match {
        case Array(sensor_id, street, lat, long, sensorType, uom, timeFormat) =>
          Some(sensor_id, (street, long, lat, sensorType, uom))
        case _ => None
      }
    }

  val mil_pollution = sc.textFile(".\\dataset\\Milano\\pollution-mi\\*.csv")
    .flatMap {
      _.split(",", -1) match {
        case Array(sensor_id, time, measure) => if (time.split(" ").length == 2)
          Some((sensor_id, DateTime.parse(time.substring(0, time.indexOf(" ")), DateTimeFormat.forPattern("yyyy/MM/dd"))), measure)
        else
          Some((sensor_id, DateTime.parse(time, DateTimeFormat.forPattern("yyyy/MM/dd"))), measure)
        case _ => None
      }
    }.aggregateByKey((0d, 0))(
    (acc, value) => (acc._1 + value.toDouble, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._2, acc2._2 + acc1._2)
  ).mapValues(sum => sum._1.toDouble / sum._2)
    .flatMap {
      case ((sensor_id, date), measure) => Some(sensor_id, measure)
      case _ => None
    }.aggregateByKey((0, 0))(
    (acc, value) => (acc._1 + value.toInt, acc._2 + 1),
    (acc1, acc2) => (acc1._1 + acc2._2, acc2._2 + acc1._2)
  ).mapValues(sum => sum._1.toDouble / sum._2)
    .join(pollution_leg).flatMap {
    case (sensor_id, (measure, (street, long, lat, sensoreType, uom))) =>
      Some((long.toDouble, lat.toDouble), (street, sensoreType, measure, uom))
    case _ => None
  }

  val arrayLongLat = mil_pollution.keys.collect()

  def isInSquare(x: Double, y: Double, array: IndexedSeq[Double]): Boolean = {
    val x_array = Array(array.apply(0), array.apply(2), array.apply(4), array.apply(6), array.apply(8))
    val y_array = Array(array.apply(1), array.apply(3), array.apply(5), array.apply(7), array.apply(9))
    var flag = false
    var j = 3
    for (i <- 0 to 4) {
      if ((((y_array(i) <= y * 1.000000000000000) && (y * 1.000000000000000 < y_array(j))) || ((y_array(j) <= y * 1.000000000000000) && (y * 1.000000000000000 < y_array(i)))) &&
        (x * 1.000000000000000 > (x_array(j) - x_array(i)) * (y * 1.000000000000000 - y_array(i)) / (y_array(j) - y_array(i)) + x_array(i))) {
        flag = !flag
      }
      j = i;
    }
    flag
  }

  val spark = SparkSession
    .builder()
    .appName("test_grid")
    .master("local[*]")
    .getOrCreate()

  val requestJson = spark.read.json(".\\dataset\\Milano\\milano-grid\\milano-grid.geojson")
    .select(col("features"))
    .withColumn("features", explode(col("features")))
    .select(col("features.*")).select(col("id"), explode(col("geometry.coordinates")))
    .rdd.flatMap {
    case row => Some(row(0).asInstanceOf[Long], row.get(1).asInstanceOf[mutable.WrappedArray[mutable.WrappedArray[Double]]])
    case _ => None
  }.mapValues(array => array.flatten).join(area)
    .flatMap {
      case (square_id, (array, status)) => {
        var long_sensor = 0d
        var lat_sensor = 0d
        arrayLongLat.foreach(longLat => if (isInSquare(longLat._1, longLat._2, array.toArray)) {
          long_sensor = longLat._1
          lat_sensor = longLat._2
        }
        )
        Some((long_sensor, lat_sensor), (square_id, status))
      }
      case _ => None
    }.join(mil_pollution).flatMap {
    case ((long, lat), ((square_id, status), (street, sensoreType, measure, uom))) =>
      Some((square_id, status), (street, sensoreType, measure, uom))
    case _ => None
  }

  val overConcentrationAreas = requestJson.flatMap {
    case ((square_id, status), (street, sensoreType, measure, uom)) => {
      if (sensoreType.equals("Ammonia") && measure > 0.4)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("Benzene") && measure > 0.8)
        Some((status, street, sensoreType, measure, uom, measure * 3), square_id)
      else if (sensoreType.equals("Nitrogene Dioxide") && measure > 0.06)
        Some((status, street, sensoreType, measure, uom, measure * 2), square_id)
      else if (sensoreType.equals("Sulfur Dioxide") && measure > 0.05)
        Some((status, street, sensoreType, measure, uom, measure * 3), square_id)
      else if (sensoreType.equals("BlackCarbon") && measure > 4)
        Some((status, street, sensoreType, measure, uom, measure * 2), square_id)
      else if (sensoreType.equals("Carbon Monoxide") && measure > 3)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("PM10 (SM2005)") && measure > 0.06)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("PM2.5") && measure > 0.0354)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("Ozono") && measure > 0.03)
        Some((status, street, sensoreType, measure, uom, measure * 4), square_id)
      else
        None
    }
    case _ => None
  }.groupByKey().flatMap {
    case ((status, street, sensoreType, measure, uom, totalDang), square_id) =>
      Some((status, totalDang), (street, sensoreType, measure, uom, square_id))
    case _ => None
  }.filter(area => area._1._2 > 7).sortByKey().coalesce(2).saveAsTextFile(".\\src\\main\\test\\overConcentr1.txt")

  val normalConcentrationAreas = requestJson.flatMap {
    case ((square_id, status), (street, sensoreType, measure, uom)) => {
      if (sensoreType.equals("Ammonia") && measure <= 0.4)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("Benzene") && measure <= 0.8)
        Some((status, street, sensoreType, measure, uom, measure * 3), square_id)
      else if (sensoreType.equals("Nitrogene Dioxide") && measure <= 0.06)
        Some((status, street, sensoreType, measure, uom, measure * 2), square_id)
      else if (sensoreType.equals("Sulfur Dioxide") && measure <= 0.05)
        Some((status, street, sensoreType, measure, uom, measure * 3), square_id)
      else if (sensoreType.equals("BlackCarbon") && measure <= 4)
        Some((status, street, sensoreType, measure, uom, measure * 2), square_id)
      else if (sensoreType.equals("Carbon Monoxide") && measure <= 3)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("PM10 (SM2005)") && measure <= 0.06)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("PM2.5") && measure <= 0.0354)
        Some((status, street, sensoreType, measure, uom, measure * 1), square_id)
      else if (sensoreType.equals("Ozono") && measure <= 0.03)
        Some((status, street, sensoreType, measure, uom, measure * 4), square_id)
      else
        None
    }
    case _ => None
  }.groupByKey().flatMap {
    case ((status, street, sensoreType, measure, uom, totalDang), square_id) =>
      Some((status, totalDang), (street, sensoreType, measure, uom, square_id))
    case _ => None
  }.sortByKey().saveAsTextFile(".\\src\\main\\test\\normalConcentr.txt")


  spark.stop()
  sc.stop
}
