package dk.zxd.test

import dk.zxd.util.XMLParser
import org.junit.Test
import org.junit.Assert

/**
  * Created by zxd on 5/4/17.
  */
class XMLParseTest {
    /*
    <tables>
        <table>
            <name>c3</name>
            <input>file:///home/gpt43t/zxd/data/20170414/c3_succ.log.2017-04-14-14</input>
            <format>text</format>
            <schema>file:///home/gpt43t/zxd/spark/cert/pos_name_si2.txt</schema>
            <partitions>2000</partitions>
        </table>
     </tables>
     */
    @Test
    def testTableParse(): Unit = {
        val path = "conf/test_table.xml"
        val tables = XMLParser.tableParse(path)
        Assert.assertEquals(1, tables.size)
        val table = tables(0)
        Assert.assertEquals("file:///home/gpt43t/zxd/data/20170414/c3_succ.log.2017-04-14-14", table.inputPath)
        Assert.assertEquals("text", table.inputFormat)
        Assert.assertEquals(2000, table.partitions)
        Assert.assertEquals("file:///home/gpt43t/zxd/spark/cert/pos_name_si2.txt", table.schemaPath)
        Assert.assertEquals("c3", table.tableName)
    }

    /*
    <tasks>
        <task>
            <sqlTask>select count(distinct 66_gl_sip) from c3</sqlTask>
            <input>c3</input>
            <outputName>66_gl_sip_cnt</outputName>
            <outputDir>file:///home/gpt43t/zxd/data/</outputDir>
            <outputFormat>text_tab</outputFormat>
            <saveSchema>true</saveSchema>
            <cacheAble>false</cacheAble>
        </task>
    </tasks>
     */
    @Test
    def testTaskParse(): Unit = {
        val path = "conf/test_task.xml"
        val tasks = XMLParser.taskParse(path)
        Assert.assertEquals(1, tasks.size)
        val task = tasks(0)
        Assert.assertEquals(1, task.dependencyTable.length)
        Assert.assertEquals("c3", task.dependencyTable(0))
        Assert.assertEquals("file:///home/gpt43t/zxd/data/", task.outputDir)
        Assert.assertEquals("text_tab", task.outputFormat)
        Assert.assertEquals("66_gl_sip_cnt", task.outputName)
        Assert.assertEquals(true, task.saveSchema)
        Assert.assertEquals("select count(distinct 66_gl_sip) from c3", task.sqlTask)
    }

    /*
    <conf>
        <memory>32G</memory>
        <cores>12</cores>
        <sparkParam>
            <key>spark.master</key>
            <value>spark://localhost:7077</value>
        </sparkParam>
        <!--<platformParam></platformParam>-->
    </conf>
     */
    @Test
    def testConfParse(): Unit = {
        val path = "conf/test_conf.xml"
        val conf = XMLParser.confParse(path)
        Assert.assertEquals("12", conf.sparkParams("spark.cores.max"))
        Assert.assertEquals("32G", conf.sparkParams("spark.executor.memory"))
        Assert.assertEquals(true, conf.platformParams.isEmpty)
    }
}