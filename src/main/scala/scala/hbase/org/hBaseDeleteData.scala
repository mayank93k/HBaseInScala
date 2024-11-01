package scala.hbase.org

import org.apache.hadoop.hbase.client.{ConnectionFactory, Delete, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.log4j.{Level, Logger}

object hBaseDeleteData {
  System.setProperty("hadoop.home.dir", "C:\\winutils")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Delete Data Starts")
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "hbasehost")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    val connection = ConnectionFactory.createConnection(conf)

    //**********************Deletion of HBase Table Data********************
    val tableName: String = "employee"
    val table = TableName.valueOf(tableName)
    val getTableName = connection.getTable(table)
    val delete = new Delete(Bytes.toBytes("key_1000"))
    getTableName.delete(delete)
    val get = new Get(Bytes.toBytes("key_1000"))
    val result = getTableName.get(get)
    println("result: " + result)
    if (result.value() == null) {
      println("Delete Data is Successful")
    }
    getTableName.close()
    connection.close()
    println("Deletion of Table Done")
  }
}
