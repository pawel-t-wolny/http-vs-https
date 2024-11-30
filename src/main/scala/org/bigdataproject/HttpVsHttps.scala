package org.bigdataproject

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.fs.{FileSystem, Path}
import org.bigdataproject.CountryCodes.countryCodes
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.net.URL

object HttpVsHttps {
    def main(args: Array[String]) {
        val sparkConf = new SparkConf()
              .setAppName("HTTP vs HTTPS")
              .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .registerKryoClasses(Array(classOf[WarcRecord]))

        val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        val sparkCtx = sparkSession.sparkContext
        
        val warcFolderPath = "hdfs:///single-warc-segment"
        val numberOfFiles = 640 // All the files in the single-warc-segment directory

        val hadoopFileSystem = FileSystem.get(sparkCtx.hadoopConfiguration)
        val warcFiles = hadoopFileSystem.listStatus(new Path(warcFolderPath))
                            .filter(_.getPath.getName.endsWith(".warc.gz"))
                            .take(numberOfFiles)
                            .map(_.getPath.toString)

        val warcs = sparkCtx.newAPIHadoopFile(
                        warcFiles.mkString(","),
                        classOf[WarcGzInputFormat],            
                        classOf[NullWritable],                  
                        classOf[WarcWritable]                   
                    )

        val broadcastCountryCodes = sparkCtx.broadcast(countryCodes)

        val protocolCounts = warcs.map{ wr => wr._2 }
                .filter{ _.isValid() }
                .map{ _.getRecord().getHeader() }
                .filter{ _.getHeaderValue("WARC-Type") == "response" }
                .map{ wh => wh.getUrl() }
                .map{ url => 
                    val urlObj = new URL(url)
                    val host = urlObj.getHost()
                    val protocol = urlObj.getProtocol()
                    val domainParts = host.split("\\.")
                    if (domainParts.length > 1) { 
                        val tld = domainParts.takeRight(1).mkString(".")
                        if (protocol == "http") (0, tld) 
                        else (1, tld)
                    }
                    else host
                }
                .filter{case(_, tld: String) => 
                    val set = broadcastCountryCodes.value
                    set.contains(tld)
                }
                .cache()

        val totalCounts = protocolCounts.map { case (_, cc) => (cc, 1) }
            .reduceByKey(_ + _)

        val httpsCounts = protocolCounts.filter { case (protocol, _) => protocol == 1 }
            .map { case (_, cc) => (cc, 1) }
            .reduceByKey(_ + _)

        val joinedCounts = totalCounts.leftOuterJoin(httpsCounts)

        val percentages = joinedCounts.map { case (cc, (total, httpsOpt)) =>
            val https = httpsOpt.getOrElse(0)
            val http = total - https
            val httpPercentage = 100.0 * http / total
            val httpsPercentage = 100.0 * https / total
            (cc, httpPercentage, httpsPercentage, total)
        }

        val sortedPercentages = percentages.sortBy(_._3, ascending = false)

        println("\n########## OUTPUT ##########")
        sortedPercentages.collect().foreach { case (cc, httpPct, httpsPct, total) =>
            println(f"Country Code: $cc, HTTP: $httpPct%.1f%%, HTTPS: $httpsPct%.1f%%, Count: $total")
        }
        println("\n########### END ############")

        sparkCtx.stop()
    }
}