package dk.zxd.test

import dk.zxd.util.FileHelper
import org.junit.{Assert, Before, Test}

import scala.io.Source

/**
  * Created by zxd on 5/4/17.
  */
class FileHelperTest {
    val BASE_HDFS_URI = "hdfs://master:9000"
    @Before
    def setUp(): Unit = {
        FileHelper.initHdfs(BASE_HDFS_URI)
    }

    @Test
    def testGetFileSize(): Unit = {
        val size1 = FileHelper.getFilesSize(BASE_HDFS_URI + "/zxd/cert/c3_succ.log.2017-04-14-14")
        val size2 = FileHelper.getFilesSize("file:///home/gpt43t/zxd/data/20170414/c3_succ.log.2017-04-14-14")
        println(s"$size1\t$size2")
        Assert.assertEquals(12338023917l, size1)
        Assert.assertEquals(12338023917l, size2)
    }

    @Test
    def testSave(): Unit = {
        val path = "/home/gpt43t/zxd/spark/cert/pos_name_si2.txt"
        val size = FileHelper.getFilesSize("file://" + path)
        val data = Source.fromFile(path).getLines().mkString("\r\n")
        FileHelper.save("file:///home/gpt43t/zxd/spark/cert/file_helper_test_save_output", data)
        FileHelper.save(BASE_HDFS_URI + "/zxd/cert/file_helper_test_save_output", data)
        val size1 = FileHelper.getFilesSize("file:///home/gpt43t/zxd/spark/cert/file_helper_test_save_output")
        val size2 = FileHelper.getFilesSize(BASE_HDFS_URI + "/zxd/cert/file_helper_test_save_output")
        println(s"$size\t$size1\t$size2")
        Assert.assertEquals(size, size1)
        Assert.assertEquals(size, size2)
    }
}
