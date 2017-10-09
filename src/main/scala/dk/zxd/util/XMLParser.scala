package dk.zxd.util

import dk.zxd.model.{Conf, Table, Task}
import org.apache.log4j.Logger

import scala.xml.{NodeSeq, XML}

/**
 * Created by zxd on 2017/4/24.
 */
object XMLParser {
    private val logger = Logger.getLogger(XMLParser.getClass)

    def tableParse(tableXmlFile: String) = {
        val tableXml = XML.loadFile(tableXmlFile)
        if (!tableXml.label.equals("tables")) {
            logger.error("table xml without root label <tables>")
            throw new IllegalArgumentException
        }
        (tableXml \ "table").map(node => {
            val tableName = (node \ "name").text
            val inputPath = (node \ "input").text
            val inputFormat = (node \ "format").text
            val schemaPath = getText(node \ "schema", null)
            val partitions = getText(node \ "partitions", null)
            logger.info(s"tableParse: tableName: ${tableName}\tinput: ${inputPath}\tinputFormat: ${inputFormat}")
            if (partitions == null) Table(tableName, inputPath, schemaPath, inputFormat)
            else Table(tableName, inputPath, schemaPath, inputFormat, partitions.toInt)
        })
    }

    def taskParse(taskXmlFile: String) = {
        val taskXml = XML.loadFile(taskXmlFile)
        if (!taskXml.label.equals("tasks")) {
            logger.error("table xml without root label <tasks>")
            throw new IllegalArgumentException
        }
        (taskXml \ "task").map(node => {
            val sqlTask = (node \ "sqlTask").text
            val dependencyTable = (node \ "input").text.split(",").map(item => item.trim)
            if (dependencyTable.toSet.size != dependencyTable.length) {
                logger.error("every name list in '<input>' must be unique")
            }
            val outputName = (node \ "outputName").text
            val task = new Task(sqlTask, dependencyTable, outputName)
            node.child.foreach({
                case <outputDir>{text}</outputDir> => task.outputDir = text.text
                case <outputFormat>{text}</outputFormat> => task.outputFormat = text.text
                case <saveSchema>{text}</saveSchema> => task.saveSchema = text.text.toBoolean
                case <cacheAble>{text}</cacheAble> => task.cacheAble = text.text.toBoolean
                case <overwrite>{text}</overwrite> => task.overwrite = text.text.toBoolean
                case _ =>
            })
            if (task.outputDir != null &&
                    !(task.outputDir.startsWith("file://") || task.outputDir.startsWith("hdfs://"))) {
                logger.error("Unknown protocol, only 'hdfs://' or 'file://' can be accepted")
                throw new IllegalArgumentException
            }
            logger.info(s"taskParse: ${task}")
            task
        })
    }

    def confParse(confXmlFile: String) = {
        val confXml = XML.loadFile(confXmlFile)
        if (!confXml.label.equals("conf")) {
            logger.error("table xml without root label <conf>")
            throw new IllegalArgumentException
        }
        val memory = (confXml \ "memory").text
        val cores = (confXml \ "cores").text.toInt
        val master = (confXml \ "master").text
        val conf = Conf(memory, cores, master)
        (confXml \ "sparkParam").foreach(x => {
            conf.sparkParams += (x \ "key").text -> (x \ "value").text
        })
        (confXml \ "platformParam").foreach(x => {
            conf.sparkParams += (x \ "key").text -> (x \ "value").text
        })
        logger.info(s"confParse: ${conf}")
        conf
    }

    private def getText(node: NodeSeq, default: String) = {
        if (node != null && node.nonEmpty) node.text
        else default
    }
}
