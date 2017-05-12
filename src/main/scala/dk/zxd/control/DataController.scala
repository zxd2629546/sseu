package dk.zxd.control

import dk.zxd.model.{Job, Table, Task}
import dk.zxd.util.FileHelper
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Encoders, Row, SparkSession}

/**
 * Created by zxd on 2017/4/25.
 */
object DataController {
    type IndexNameType = (Int, String, String)
    private val logger = Logger.getLogger(DataController.getClass)

    def loadAndRegist(spark: SparkSession, table: Table) = {
        var posName: List[IndexNameType] = null
        var schema: StructType = null
        var index: Broadcast[List[IndexNameType]] = null

        if (table.schemaPath != null) {
            posName = getPosName(spark, table.schemaPath)
            schema = createSchema(posName)
            index = spark.sparkContext.broadcast(posName)
        }
        table.inputFormat match {
            case Table.CSV_FORMAT => {
                val inputFile = spark.read.option("header", "true")
                        .csv(table.inputPath).repartition(table.partitions)
                val encoder = RowEncoder(inputFile.schema)
                val data = inputFile.map(row => Row.fromSeq(
                    index.value.map(idx => cast(row(idx._1 - 1), idx._3))
                ))(encoder).rdd
                registView(spark, data, schema, table)
            }
            case Table.JSON_FORMAT => {
                val data = spark.read.json(table.name()).repartition(table.partitions)
                registView(data, table)
            }
            case Table.TEXT_FORMAT => {
                val data = loadText(spark, table, index, "\\s+")
                registView(spark, data, schema, table)
            }
            case Table.TEXT_SPACE_FORMAT => {
                val data = loadText(spark, table, index, " ")
                registView(spark, data, schema, table)
            }
            case Table.TEXT_TAB_FORMAT => {
                val data = loadText(spark, table, index, "\t")
                registView(spark, data, schema, table)
            }
            case Table.TEXT_COMMA_FORMAT => {
                val data = loadText(spark, table, index, ",")
                registView(spark, data, schema, table)
            }
        }
        logger.info(s"${table.name()} load and regist complete")
    }

    private def cast(x: Any, t: String) = {
        if (x == null) x
        else {
            try {
                t match {
                    case "i" => x.toString.toInt
                    case "l" => x.toString.toLong
                    case "f" => x.toString.toDouble
                    case _ => x
                }
            } catch {
                case e: Exception => {
                    logger.error(s"x:$x\ttype:$t")
                    0
                }
            }
        }
    }

    private def loadText(spark: SparkSession, table: Table,
                         index: Broadcast[List[IndexNameType]],
                         splitRegx: String) = {
        logger.info(s"load table:${table.name()} with $splitRegx and ${table.partitions} partitions")
        spark.sparkContext.textFile(table.inputPath, table.partitions)
          .map(line => {
            val items = line.split(splitRegx)
            Row.fromSeq(index.value.map(idx => cast(items(idx._1 - 1), idx._3)))
        })
    }

    def registView(spark: SparkSession, data: RDD[Row], schema: StructType, table: Table): Unit = {
        registView(spark.createDataFrame(data, schema), table)
    }

    def registView(data: DataFrame, job: Job): Unit = {
        logger.info(s"regist ${job.name()}")
        data.createOrReplaceTempView(job.name())
    }

    def cache(spark: SparkSession, job: Job, cache: Boolean): Unit = {
        val name = job.name()
        val isCached = if(spark.catalog.tableExists(name)) spark.catalog.isCached(name) else false
        logger.info(s"$name isCached:$isCached")
        if (cache) {
            if (!isCached) {
                spark.catalog.cacheTable(job.name())
                logger.info(s"cache ${job.name()}")
            }
        } else {
            if (isCached) {
                spark.catalog.uncacheTable(job.name())
                logger.info(s"uncache ${job.name()}")
            }
        }
    }

    def calculate(spark: SparkSession, task: Task) = {
        spark.sql(task.sqlTask)
    }

    def save(spark: SparkSession, data: DataFrame, task: Task) = {
        if (task.outputDir != null || task.outputFormat != null) {
            val savePath = task.outputDir + task.outputName
            logger.info(s"save ${task.name()} to $savePath")
            val writer = data.write.mode("overwrite")
            task.outputFormat match {
                case Table.CSV_FORMAT => {
                    writer.option("header", "true").csv(savePath)
                    if (task.saveSchema) saveSchema(data.dtypes, task)
                }
                case Table.JSON_FORMAT => {
                    writer.json(savePath)
                }
                case Table.TEXT_TAB_FORMAT => {
                    val encoder = Encoders.STRING
                    data.map(row => row.toSeq.mkString("\t"))(encoder)
                      .write.mode("overwrite").text(savePath)
                    if (task.saveSchema) saveSchema(data.dtypes, task)
                }
                case _ => {
                    logger.error("wrong output format, only [json, csv, text_tab] can be accepted")
                    throw new IllegalArgumentException
                }
            }
            logger.info("save complete")
            if (savePath.startsWith("file://")) {
                logger.info("merge results in local file system")
                FileHelper.getMerge(savePath)
            }
        }
    }

    private def saveSchema(schema: Array[(String, String)], task: Task): Unit = {
        val savePath = task.outputDir + task.outputName + "_schema"
        logger.info(s"save ${task.outputName}'s schema to $savePath")
        FileHelper.save(savePath,
            schema.zipWithIndex.map(item => {
                s"${item._2}\t${item._1._1}\t${item._1._2}"
            }).mkString("\n"))
        logger.info("save schema complete")
    }

    private def getPosName(spark: SparkSession, schemaPath: String) = {
        val posName = spark.sparkContext.textFile(schemaPath).map(line => {
            val items = line.split("\\s+")
            val pos = items(0).split("_")(0).toInt
            val name = items(1)
            val si = items(2).toLowerCase.substring(0, 1)
            (pos, name, si)
        }).collect().toList
        logger.debug(s"posName in $schemaPath:\n${posName.mkString("\n")}")
        posName
    }

    private def createSchema(posName: List[IndexNameType]) = {
        val schema = StructType(posName.map(item => {
            item._3 match {
                case "i" => StructField(item._2, IntegerType, true)
                case "l" => StructField(item._2, LongType, true)
                case "f" => StructField(item._2, DoubleType, true)
                case _ => StructField(item._2, StringType, true)
            }
        }))
        logger.debug(s"schema:\n$schema")
        schema
    }
}
