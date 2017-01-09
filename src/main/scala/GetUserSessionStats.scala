/**
  * Input: the gzipped csv file (space-separated) located in the data/ folder of the repo. The name has been hard-coded here, but can be changed to be read from config later on.
  * Output: Writes out a json file, and the output Dataset[UserSessionStats] in parquet format to data/ folder (appended by present timestamp in millis)
  * Dependencies: See build.sbt
  * Function: The goal of this program is to read in the gz file, and output a few descriptive statistics about the data.
  * The session duration (in mins) can be optionally passed in as an arg (Integer), and if nothing is passed in, assumes 15 mins.
  * Run this from within the repo using: 
  * "sbt package", followed by
  * "/usr/local/Cellar/apache-spark/2.1.0/bin/spark-submit --class "GetUserSessionStats" --master local[4] target/scala-2.11/paytmweblogchallenge_2.11-1.0.jar <sessionDurInMins: Option[Int]>"
  *
  *
  * Created by Sila Dey on 1/9/2017
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
    import org.json4s._
    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._
    import java.io._
    import scala.util.Try

    object GetUserSessionStats {

        def GetUserSessionDataset(
            sqlc: SQLContext
            , inDf: DataFrame
            , inSessionTimeMins: Int = 15
            ) = {
            /** Input: Raw Dataframe, and input session time in minutes (default of 15 mins)
              * Output: The per-user dataset of descriptive statistics, Dataset[UserSessionStats]
              * , and the Mapping of session start time in millis to sessionIdx
              * Function: This function does pre-processing on a few columns (ts, request, client_port)
              * , converts it into per-user Dataset, and calculates the per-user statistics of session duration
              * (max, as well as average). 
              * The transformation to timestamp is the one that needs little explanation. 
              * First, the timestamp is converted to millis, and then session indices are created in 
              * intervals of (inSessionTimeMins, default 15 mins), and these session indices are used throughout.
              * However, as mentioned above, one of the outputs is also the mapping of the session start times to their index
              * 
              */
            import sqlc.implicits._

            val dfTmp1 = inDf.where(col("elb_status")===200)
                    .select(col("ts"), col("client_port"), col("user_agt"), col("request"))

            val tsMin_tmp = new DateTime(dfTmp1.select(min(col("ts"))).take(1)(0).getAs[java.sql.Timestamp](0))
            val tsMin = new DateTime(tsMin_tmp.getYear, tsMin_tmp.getMonthOfYear, tsMin_tmp.getDayOfMonth, tsMin_tmp.getHourOfDay, tsMin_tmp.getMinuteOfHour)
            
            val tsMax_tmp = new DateTime(dfTmp1.select(max(col("ts"))).take(1)(0).getAs[java.sql.Timestamp](0))
            val tsMax = new DateTime(tsMax_tmp.getYear, tsMax_tmp.getMonthOfYear, tsMax_tmp.getDayOfMonth, tsMax_tmp.getHourOfDay, tsMax_tmp.getMinuteOfHour)

            // Create map of session start time to session index
            val mapSessStartToSessIdx = (tsMin.getMillis to tsMax.getMillis by inSessionTimeMins*60*1000).zipWithIndex.toMap

            val convertTimestampToMillisUDF = udf( (inTs: Timestamp) => {
                val cal = Calendar.getInstance()
                cal.setTime(inTs)
                cal.getTimeInMillis
            })

            // UDF to retain only the relevant portion of URL upto "?"
            val cropRequestToUrlUDF = udf( (inStr: String) => {
                if (inStr.length > 0) Some(inStr.split(" ")(1).split("\\?")(0)) else None
            } )

            // UDF to retain only IP, and drop specific PORT info
            val getClientUrlFromClientPortUDF = udf( (inPort: String) => (inPort.split(":")(0)) )

            // UDF to convert the ts in millis to session index
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

            // Transform evented data to per-user format by grouping-by IP (or client)
            val dsTmp1 = dfTmp2.rdd.map(r => 
                (r.getString(2), Tuple2(r.getLong(3),r.getString(1)) ) 
                ).toDS
                .groupBy("_1")
                .agg(collect_list(col("_2")))
                .withColumnRenamed("_1", "client")
                .withColumnRenamed("collect_list(_2)", "sessId_req")
                .as[(String, Seq[(Long, String)])]

            // Transform the per-user dataset for each of the below statistics
            // 1. session history
            // 2. session unique visited urls
            // 3. session streak history
            // 4. session average duration
            // 5. session max duration
            // case class UserSessionStats describes the expected schema as well
            val dsFinal = dsTmp1.map({ case (ip, seq_sessid_url) => (ip, 
                    seq_sessid_url.groupBy(_._1).map({ case x => (x._1, x._2.map(_._2))})) 
                })
                .map({ case (ip, map_sessHist) => (ip, map_sessHist, 
                    map_sessHist.map({ case (k,v) => (k, v.toSet.toSeq) })
                )})
                .map({ case (ip, map_sessHist, map_sessUniqueUrls) => 
                    (ip, map_sessHist, map_sessUniqueUrls, 
                map_sessUniqueUrls.map(_._1.toInt).toArray.sortWith(_<_) ) })
                .map({case (ip, map_sessHist, map_sessUniqueUrls, arr_sessid) => 
                    (ip, map_sessHist, map_sessUniqueUrls, 
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
                    }) 
                })
                .map({ case (ip, map_sessHist, map_sessUniqueUrls, arr_sess_hist) => 
                    (ip, map_sessHist, map_sessUniqueUrls, arr_sess_hist, 
                    (arr_sess_hist.getOrElse(Array(0)).sum.toDouble * inSessionTimeMins)/arr_sess_hist.getOrElse(Array(0)).size.toDouble,
                    (arr_sess_hist.getOrElse(Array(0)).max * inSessionTimeMins) )
                })
                .withColumnRenamed("_1", "client")
                .withColumnRenamed("_2", "sessHistory")
                .withColumnRenamed("_3", "sessUniqueUrls")
                .withColumnRenamed("_4", "sessDurHistOption")
                .withColumnRenamed("_5", "sessAvgDur")
                .withColumnRenamed("_6", "sessMaxDur")
                .as[UserSessionStats]

            (dsFinal, mapSessStartToSessIdx)
        }

        def writeStats(
            inDs: Dataset[UserSessionStats]
            , mapSessStartTimeToSessIdx: Map[Long, Int]
            ,  inSessionTimeMins: Int = 15
            ) = {
            /**
              * Input: the final dataset with requisite schema, Dataset[UserSessionStats]
              * , and the session duration chosen
              * Output: Unit
              * Function: This function writes out the stats to the data/ in the repo,
              * specifically : 1. JSON file with requisite info, 2. parquet output Dataset[UserSessionStats]
              * The function also prints the JSON file to console
              */
            val maxSessDur : Int = inDs.agg(max(col("sessMaxDur"))).take(1)(0).getInt(0)
            val avgSessDur : Double = inDs.agg(avg(col("sessAvgDur"))).take(1)(0).getDouble(0)

            // Filter all users who have the maximum session duration of all users (happens to be only 1)
            val listEngagedUsers = inDs
                .where(col("sessMaxDur")===maxSessDur)
                .select(col("client"))
                .collect
                .map(_.toString)

            implicit val formats = DefaultFormats

            val outJson = compact(render(
                ("chosenSessionTime" -> inSessionTimeMins)
                ~ ("numUsers" -> inDs.count)
                ~ ("maxSessDur" -> maxSessDur) 
                ~ ("avgSessDur" -> avgSessDur)
                ~ ("listEngagedUsers" -> Extraction.decompose(listEngagedUsers))
                ~ ("mapSessStartTimeToSessIdx", Extraction.decompose(mapSessStartTimeToSessIdx))
                ))

            val outTs = System.currentTimeMillis
            inDs.write.parquet("data/userStatsDataset_"+outTs)
            new PrintWriter(new File("data/userStatsJson_"+outTs+".json")) {
                write(outJson)
                close
            }
            println(s"outJson: ${pretty(render(outJson))}")
        }

        def main(args: Array[String]): Unit = {
            // TODO: Make this into arg, along with the requisite work to handle crappy inputs
            val argSessionTimeMins : Int = if (Try(args(0).toInt).isSuccess) args(0).toInt else 15

            val conf = new SparkConf().setAppName("User-Session-Stats-Builder")
            val sc = new SparkContext(conf)
            val sqlc = new org.apache.spark.sql.SQLContext(sc)
            val spark = SparkSession.builder.getOrCreate()

            val inFilename : String = "data/2015_07_22_mktplace_shop_web_log_sample.log.gz"

            // Explicit schema definition of input file
            // Reference: http://docs.aws.amazon.com/ElasticLoadBalancing/latest/DeveloperGuide/access-log-collection.html#access-log-entry-format
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

            val (dsOut, mapSessStartTimeToSessIdx) = GetUserSessionDataset(sqlc, dfIn, argSessionTimeMins)
            writeStats(dsOut, mapSessStartTimeToSessIdx, argSessionTimeMins)
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
