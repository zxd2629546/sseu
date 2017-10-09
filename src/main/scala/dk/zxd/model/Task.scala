package dk.zxd.model

/**
 * Created by zxd on 2017/4/24.
 */
class Task(val sqlTask: String,
           val dependencyTable: Array[String],
           val outputName : String) extends Job{
    var outputDir: String = null
    var outputFormat: String = Table.TEXT_TAB_FORMAT
    var saveSchema: Boolean = true
    var overwrite: Boolean = true

    override def name(): String = outputName

    def needSave = if(outputDir == null) false else true

    override def toString = s"Task(outputDir=$outputDir, outputFormat=$outputFormat, saveSchema=$saveSchema, sqlTask=$sqlTask, dependencyTable=$dependencyTable, outputName=$outputName)"
}
