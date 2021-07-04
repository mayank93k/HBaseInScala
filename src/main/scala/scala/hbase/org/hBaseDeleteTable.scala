package spark.practice.mayank.hbase

import java.io.IOException

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory}
import org.apache.log4j.{Level, Logger}

object hBaseDeleteTable {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Delete Table Starts")
    var connection: Connection = null
    var admin: Admin = null
    var tableName: String = "employee"
    //**********************Deleting HBase Table********************

    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "hbasehost")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      connection = ConnectionFactory.createConnection(conf)
      val table = TableName.valueOf(tableName)
      admin = connection.getAdmin
      admin.disableTable(table)
      admin.deleteTable(table)
      if (!admin.tableExists(table)) {
        println("Table is deleted")
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      try {
        if (admin != null) admin.close()
        if (connection != null) connection.close()
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
    println("Deletion of Table Done")
  }
}