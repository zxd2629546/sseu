package dk.zxd.model

import dk.zxd.util.FileHelper
import org.apache.log4j.Logger

/**
 * Created by zxd on 2017/4/24.
 */
/**
 * this class describes some source data which needed in the whole job
 * @param tableName an unique table name among all the tables and tasks
 * @param inputPath is the input path for the data, only 'file://' or 'hdfs://' can be accepted
 * @param schemaPath is the schema path(file://' or 'hdfs://') for the data, it can be null while inputFormat is json
 * @param inputFormat is the format for the data, only [json, csv, text, text_space, text_tab, text_comma] can be accepted
 */
class Table(val tableName: String,
            val inputPath: String,
            val schemaPath: String,
            val inputFormat: String) extends Job {
    val loggger = Logger.getLogger(Table.getClass)
    var partitions: Int = 0

    def calculatePartitions() : Unit = {
        //TO-DO: get input file size, calculate partitions
        val fileSize = FileHelper.getFilesSize(inputPath)
        partitions = Math.max(Math.ceil(fileSize / (32 * 1024 * 1024l)).toInt, 1)
        loggger.info(s"fileSize:$fileSize\tpartitions:$partitions")
    }

    override def name(): String = tableName


    override def toString = s"Table(partitions=$partitions, tableName=$tableName, inputPath=$inputPath, schemaPath=$schemaPath, inputFormat=$inputFormat)"
}

object Table {
    val JSON_FORMAT = "json"
    val CSV_FORMAT = "csv"
    val TEXT_FORMAT = "text"
    val TEXT_SPACE_FORMAT = "text_space"
    val TEXT_TAB_FORMAT = "text_tab"
    val TEXT_COMMA_FORMAT = "text_comma"

    def apply(tableName: String, inputPath: String,
              schemaPath: String, inputFormat: String) = {
        val table = new Table(tableName, inputPath, schemaPath, inputFormat)
        table.calculatePartitions()
        table
    }

    def apply(tableName: String, inputPath: String,
              schemaPath: String, inputFormat: String, partitions: Int) = {
        val table = new Table(tableName, inputPath, schemaPath, inputFormat)
        table.partitions = partitions
        table
    }
}