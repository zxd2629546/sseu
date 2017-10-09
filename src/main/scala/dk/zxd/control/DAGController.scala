package dk.zxd.control

import dk.zxd.util.{DAGHelper, FileHelper, XMLParser}
import dk.zxd.model._
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Created by zxd on 2017/4/26.
 */
class DAGController(
                     conf: Conf,
                     tables: Seq[Table],
                     tasks: Seq[Task],
                     dagHelper: DAGHelper) {
    private val logger = Logger.getLogger(DAGController.getClass)
    var spark: SparkSession = _

    def init(): Unit = {
        //init spark
        logger.info("init DAG Controller")
        var builder = SparkSession.builder()
        conf.sparkParams.foreach(item => builder = builder.config(item._1, item._2))
        builder.master(conf.master)
        logger.info(s"set master at ${conf.master}")
        spark = builder.appName(DAGController.APP_NAME).getOrCreate()
        dagHelper.init()
        dagHelper.order()
        logger.info("init complete")
    }

    def schedule() = {
        val jobs = dagHelper.topSorted
        val outCnt = dagHelper.outCnt.clone()
        jobs.foreach(job => {
            val id = dagHelper.id(job)
            job match {
                case table: Table => {
                    if (outCnt(id) > 0) {
                        DataController.loadAndRegist(spark, table)
                        if (outCnt(id) > 1) {
                            DataController.cache(spark, table, true)
                            uncacheParent(spark, id, outCnt)
                        }
                    }
                }
                case task: Task => {
                    if (outCnt(id) > 0 || task.needSave) {
                        val data = DataController.calculate(spark, task)
                        if (outCnt(id) > 0) {
                            DataController.registView(data, task)
                            if (outCnt(id) > 1) {
                                DataController.cache(spark, task, true)
                                uncacheParent(spark, id, outCnt)
                            }
                        }
                        if (task.needSave){
                            DataController.save(spark, data, task)
                            uncacheParent(spark, id, outCnt)
                        }
                    }
                }
                case default => {
                    logger.error(default)
                    logger.error("can't match!")
                }
            }
        })
    }

    private def uncacheParent(spark: SparkSession, id: Int, outCnt: Array[Int]): Unit = {
        if (outCnt(id) == 0) {
            DataController.cache(spark, dagHelper.job(id), false)
        } else {
            dagHelper.graph.zipWithIndex.foreach(row => {
                if (row._1(id)) {
                    outCnt(row._2) -= 1
                    uncacheParent(spark, row._2, outCnt)
                }
            })
        }
    }

    def finish() = {
        spark.catalog.clearCache()
        logger.info("mission complete")
    }
}

object DAGController {
    val APP_NAME = "Spark SQL Easy Use Engine"
    def apply(params: Map[String, String]) = {
        val tableXmlPath = params("table")
        val taskXmlPath = params("task")
        val confXmlPath = params("conf")
        val hdfsNameNodeUri = if (params.contains("hdfs")) params("hdfs") else null

        val conf = XMLParser.confParse(confXmlPath)
        val tables = XMLParser.tableParse(tableXmlPath)
        val tasks = XMLParser.taskParse(taskXmlPath)
        if (hdfsNameNodeUri != null)FileHelper.initHdfs(hdfsNameNodeUri)
        val dagHelper = new DAGHelper(tables, tasks)
        new DAGController(conf, tables, tasks, dagHelper)
    }
}
