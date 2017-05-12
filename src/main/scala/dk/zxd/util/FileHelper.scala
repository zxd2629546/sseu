package dk.zxd.util

import java.io.{BufferedReader, InputStreamReader}
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger

/**
 * Created by zxd on 2017/4/24.
 */
object FileHelper {
    private val logger = Logger.getLogger(FileHelper.getClass)
    private var hdfs: FileSystem = null
    private val localFS: FileSystem = FileSystem.getLocal(new Configuration())

    def initHdfs(uri: String): Unit = {
        logger.info(s"init hdfs at $uri")
        hdfs = FileSystem.get(URI.create(uri), new Configuration())
    }

    private def getFileSystem(path: String) = {
        if (path.startsWith("file://")) {
            logger.debug(s"$path get localFileSystem")
            localFS
        } else {
            if (path.startsWith("hdfs://")) {
                logger.debug(s"$path get HDFS")
                if (hdfs != null) hdfs
                else {
                    logger.error("set hdfs uri first")
                    throw new IllegalArgumentException
                }
            } else {
                logger.error("Unknown protocol, only 'hdfs://' or 'file://' can be accepted")
                throw new IllegalArgumentException
            }
        }
    }

    private def truePath(path: String) = {
        val items = path.split("/").toList
        val truePath = s"/${items.slice(3, items.size).mkString("/")}"
        logger.debug(s"truePath=$truePath")
        truePath
    }

    def getFilesSize(path: String): Long = {
        val fs = getFileSystem(path)
        if (fs != null){
            val fsPath = new Path(truePath(path))
            val fileStatusArray = fs.globStatus(fsPath)
            logger.debug(s"${fileStatusArray.length} files in the $path")
            val size = fileStatusArray.foldLeft(0l)((b, a) => a.getLen + b)
            logger.debug(s"$path total size=$size")
            size
        } else {
            -1
        }
    }

    /**
      * this function can only save 64M data due to String length limits
      * @param path
      * @param data
      */
    def save(path: String, data: String): Unit = {
        val fs = getFileSystem(path)
        if (fs != null){
            val fsPath = new Path(truePath(path))
            val outputStream = fs.create(fsPath, true)
            outputStream.writeBytes(data)
            outputStream.flush()
            outputStream.close()
            logger.info(s"$path save complete, total length=${data.size}")
        }
    }

    def getMerge(path: String): Unit = {
        val fs = getFileSystem(path)
        val fsPath = new Path(truePath(path))
        val newPath = new Path(truePath(s"${path}_del"))

        fs.delete(newPath, true)
        fs.rename(fsPath, newPath)
        logger.info(s"rename ${fsPath.toString} to ${newPath.toString}")

        val fileStatusArray = fs.listStatus(newPath)
        val writer = fs.create(fsPath, true)
        fileStatusArray.foreach(status => {
            val reader = fs.open(status.getPath)
            var len = 1
            val buffer = new Array[Byte](1024 * 1024)
            while(len > 0) {
                len = reader.read(buffer)
                if (len > 0) {
                    writer.write(buffer, 0, len)
                }
            }
            reader.close()
        })
        writer.close()
        logger.info("merge complete")
        fs.delete(newPath, true)
        logger.info(s"delete ${newPath.toString}")
    }

    def head(path: String, n: Int): List[String] = {
        val fs = getFileSystem(path)
        val fsPath = new Path(truePath(path))
        val targetPath = fs.listStatus(fsPath)(0).getPath
        val reader = new BufferedReader(new InputStreamReader(fs.open(targetPath)))
        val lines = (1 to n).map(i => reader.readLine())
        reader.close()
        lines.toList
    }
}
