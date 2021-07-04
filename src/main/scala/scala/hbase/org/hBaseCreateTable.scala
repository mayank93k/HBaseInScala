package spark.practice.mayank.hbase

import java.io.IOException

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor}
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Admin

object hBaseCreateTable {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Create Table Starts")
    val conf = HBaseConfiguration.create()
     conf.set("hbase.zookeeper.quorum", "hbasehost")
     conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)

    //**********************Create HBase Table********************
    val admin = connection.getAdmin()
    val table = "employee"
    val tableName = TableName.valueOf(table)
    try {
      val htable = new HTableDescriptor(tableName)
      htable.addFamily(new HColumnDescriptor("personal"))
      htable.addFamily(new HColumnDescriptor("professional"))
      admin.createTable(htable)
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      admin.close()
      connection.close()
    }
    println("Creation of Htable Done")
  }
}
