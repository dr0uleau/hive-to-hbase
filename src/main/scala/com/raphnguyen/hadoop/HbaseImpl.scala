package com.raphnguyen.hadoop

import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, TableName, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.spark.sql.hive.HiveContext

class HbaseImpl extends Hbase {
    def deleteTable(tableName: String, hiveContext: HiveContext, admin: HBaseAdmin): Unit = {
        if (admin.tableExists(tableName)) {
            println("Deleting table %s...".format(tableName))
            admin.disableTable(tableName)
            admin.deleteTable(tableName)
            println("Table %s deleted successfully".format(tableName))
        }
    }

    def createTable(tableName: String, hiveContext: HiveContext): Unit = {
        val conf = HBaseConfiguration.create()
        val admin = new HBaseAdmin(conf)

        try {
            deleteTable(tableName, hiveContext, admin)

            println("Creating table %s...".format(tableName))
            val tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
            tableDescriptor.addFamily(new HColumnDescriptor("col"))
            admin.createTable(tableDescriptor)
            println("Table %s created successfully".format(tableName))
        } catch {
            case e: Exception =>
                throw new Exception(e)
        } finally {
            admin.close()
        }
    }

    def insertRecords(hbaseTable: String, hiveTable: String, uniqueKeys: String, partitions: Int, hiveContext: HiveContext): Unit = {
        val dataFrame = hiveContext.sql("select * from %s".format(hiveTable))
        val columns = dataFrame.columns
        val keys = uniqueKeys.replaceAll(" ", "").toLowerCase.split(",")
        val columnNamesToIndexes = (columns zip columns.indices.toList).toMap

        dataFrame.repartition(partitions).foreachPartition {
            iter => {
                if (iter != null) {
                    val conf = HBaseConfiguration.create()
                    val hTable = new HTable(conf, hbaseTable)

                    try {
                        hTable.setAutoFlushTo(false)
                        while (iter.hasNext) {
                            val row = iter.next()
                            val key = keys.map(columnNamesToIndexes(_)).map(row.get(_).toString).mkString("\u0001")
                            val p = new Put(Bytes.toBytes(key))

                            for (i <- columns.indices) {
                                if (row.get(i) == null) {
                                    p.add(Bytes.toBytes("col"), Bytes.toBytes(columns(i)), Bytes.toBytes(""))
                                }
                                else {
                                    p.add(Bytes.toBytes("col"), Bytes.toBytes(columns(i)), Bytes.toBytes(row.get(i).toString))
                                }
                            }
                            hTable.put(p)
                        }
                        hTable.flushCommits()
                    } catch {
                        case e: Exception =>
                            throw new Exception(e)
                    } finally {
                        hTable.close()
                    }
                }
            }
        }
    }
}