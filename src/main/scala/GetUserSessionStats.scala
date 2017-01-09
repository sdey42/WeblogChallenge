/**
* /usr/local/Cellar/apache-spark/2.1.0/bin/spark-submit --class "GetUserSessionStats" --master local[4] target/scala-2.11/paytmweblogchallenge_2.11-1.0.jar
*/

    import org.apache.spark.SparkContext
    import org.apache.spark.SparkContext._
    import org.apache.spark._
    import org.apache.spark.rdd._
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._
    import org.apache.spark.sql._
    import org.joda.time.DateTime
    import java.sql.Timestamp
    import java.util.Calendar

    object GetUserSessionStats {

        def GetUserSessionDataset(sqlc: SQLContext, inDf: DataFrame, inSessionTimeMins: Int = 15) = {
            import sqlc.implicits._

            val dfTmp1 = inDf.where(col("elb_status")===200)
                    .select(col("ts"), col("client_port"), col("user_agt"), col("request"))

            val tsMin_tmp = new DateTime(dfTmp1.select(min(col("ts"))).take(1)(0).getAs[java.sql.Timestamp](0))
            val tsMin = new DateTime(tsMin_tmp.getYear, tsMin_tmp.getMonthOfYear, tsMin_tmp.getDayOfMonth, tsMin_tmp.getHourOfDay, tsMin_tmp.getMinuteOfHour)
            
            val tsMax_tmp = new DateTime(dfTmp1.select(max(col("ts"))).take(1)(0).getAs[java.sql.Timestamp](0))
            val tsMax = new DateTime(tsMax_tmp.getYear, tsMax_tmp.getMonthOfYear, tsMax_tmp.getDayOfMonth, tsMax_tmp.getHourOfDay, tsMax_tmp.getMinuteOfHour)

            val mapSessStartToSessId = (tsMin.getMillis to tsMax.getMillis by inSessionTimeMins*60*1000).zipWithIndex.toMap

            val convertTimestampToMillisUDF = udf( (inTs: Timestamp) => {
                val cal = Calendar.getInstance()
                cal.setTime(inTs)
                cal.getTimeInMillis
            })

            val cropRequestToUrlUDF = udf( (inStr: String) => {
                if (inStr.length > 0) Some(inStr.split(" ")(1).split("\\?")(0)) else None
            } )

            val getClientUrlFromClientPortUDF = udf( (inPort: String) => (inPort.split(":")(0)) )

            val convertMillisToSessIdxUDF = udf( (inMillis: Long ) => (inMillis - tsMin.getMillis)/(inSessionTimeMins*60*1000L) )

            val dfTmp2 = dfTmp1
                .withColumn("tsMillis", convertTimestampToMillisUDF(col("ts")))
                .drop("ts")
                .drop("user_agt")
                .withColumn("req_url", cropRequestToUrlUDF(col("request")))
                .drop("request")
                .withColumn("client", getClientUrlFromClientPortUDF(col("client_port")))
                .drop("client_port")
                .withColumn("sess_id", convertMillisToSessIdxUDF(col("tsMillis")))

            val dsTmp1 = dfTmp2.rdd.map(r => 
                (r.getString(2), Tuple2(r.getLong(3),r.getString(1)) ) 
                ).toDS
                .groupBy("_1")
                .agg(collect_list(col("_2")))
                .withColumnRenamed("_1", "client")
                .withColumnRenamed("collect_list(_2)", "sessId_req")
                .as[(String, Seq[(Long, String)])]

            val dsFinal = dsTmp1.map({ case (ip, seq_sessid_url) => (ip, 
                    seq_sessid_url.groupBy(_._1).map({ case x => (x._1, x._2.map(_._2))})) 
                })
                .map({ case (ip, map_sessHist) => (ip, map_sessHist, 
                    map_sessHist.map({ case (k,v) => (k, v.toSet.toSeq) })
                )})
                .map({ case (ip, map_sessHist, map_sessUniqueUrls) => (ip, map_sessHist, map_sessUniqueUrls, 
                map_sessUniqueUrls.map(_._1.toInt).toArray.sortWith(_<_) ) })
                .map({case (ip, map_sessHist, map_sessUniqueUrls, arr_sessid) => (ip, map_sessHist, map_sessUniqueUrls, 
                {
                    if (arr_sessid.size > 0) {
                        if (arr_sessid.size == 1) {
                            Some(Array(1))
                        } else {
                            val outStreakArr = scala.collection.mutable.ArrayBuffer[Int](1)
                        
                            (0 to arr_sessid.size - 2).map({ 
                            case idx => 
                                if ( arr_sessid(idx) + 1 == arr_sessid(idx+1) ) {
                                    outStreakArr(outStreakArr.size - 1) = outStreakArr.last + 1
                                } else outStreakArr ++= Array(1)
                            })
                            Some(outStreakArr.toArray)
                        }
                    } else None 
                }) })
                .map({ case (ip, map_sessHist, map_sessUniqueUrls, arr_sess_hist) => (ip, map_sessHist, map_sessUniqueUrls, arr_sess_hist, 
                arr_sess_hist.getOrElse(Array(0)).sum.toDouble/arr_sess_hist.getOrElse(Array(0)).size.toDouble,
                arr_sess_hist.getOrElse(Array(0)).max )})
                .withColumnRenamed("_1", "client")
                .withColumnRenamed("_2", "sessHistory")
                .withColumnRenamed("_3", "sessUniqueUrls")
                .withColumnRenamed("_4", "sessDurHistOption")
                .withColumnRenamed("_5", "sessAvgDur")
                .withColumnRenamed("_6", "sessMaxDur")
                .as[UserSessionStats]

            (dsFinal, mapSessStartToSessId)

        }

        def main(args: Array[String]): Unit = {
            val conf = new SparkConf().setAppName("User-Session-Stats-Builder")
            val sc = new SparkContext(conf)
            val sqlc = new org.apache.spark.sql.SQLContext(sc)
            val spark = SparkSession.builder.getOrCreate()

            val inFilename : String = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

            val dfInSchema = new StructType()
                .add("ts", TimestampType)
                .add("elb_name", StringType)
                .add("client_port", StringType)
                .add("backend_port", StringType)
                .add("req_time", DoubleType)
                .add("backend_time", DoubleType)
                .add("resp_time", DoubleType)
                .add("elb_status", IntegerType)
                .add("backend_status", IntegerType)
                .add("rcvd_b", IntegerType)
                .add("sent_b", IntegerType)
                .add("request", StringType)
                .add("user_agt", StringType)
                .add("ssl_cipher", StringType)
                .add("ssl_protocol", StringType)

            val dfIn = spark.read.format("com.databricks.spark.csv")
                .option("inferSchema", "true").option("delimiter", " ")
                .schema(dfInSchema)
                .load(inFilename)

            val argSessionTimeMins = 15

            val (dsOut, mapSessStartTimeToSessIdx) = GetUserSessionDataset(sqlc, dfIn, argSessionTimeMins)
            println(s"***** Number of users: ${dsOut.count} *****")
            val outFilename = "/data/UserSessionStats_"+System.currentTimeMillis()+".csv"
            dsOut.write.format("com.databricks.spark.csv").save(outFilename)
        }
        
        case class UserSessionStats( 
                    client: String
                    , sessHistory: scala.collection.Map[Long, Array[String]]
                    , sessUniqueUrls: scala.collection.Map[Long, Array[String]]
                    , sessDurHistOption: Option[Array[Int]]
                    , sessAvgDur: Double
                    , sessMaxDur: Int 
                )
    }
