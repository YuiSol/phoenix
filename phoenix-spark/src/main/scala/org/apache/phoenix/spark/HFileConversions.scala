/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.spark

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.phoenix.schema.types._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.KeyValue
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.sql.Timestamp
import java.sql.Date
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HConstants
import org.apache.phoenix.mapreduce.util.PhoenixConfigurationUtil
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.phoenix.util.ColumnInfo
import org.apache.phoenix.util.SchemaUtil

class HFileConversions(sqlContext: SQLContext) extends Serializable {

  def hFileAsRowRDD(path: String, conf: Configuration = new Configuration): RDD[(String, String, String)] = {
    // Create hFileRDD from HFile Path
    val hFileRDD = sqlContext.sparkContext.newAPIHadoopFile(path, classOf[HFileInputFormat], classOf[ImmutableBytesWritable], classOf[KeyValue], conf)

    // Create rowRDD from hFileRDD
    val rowRDD = hFileRDD.mapPartitions(convertToRow, true)

    // Create a tuple with rowRDD
    rowRDD
  }

  def hFileAsDataFrameUsingTableSchema(path: String, fieldsMapping: Map[String, String], tableName: String, zkUrl: Option[String] = None,
    conf: Configuration = new Configuration): DataFrame = {
    // Create a tuple with rowRDD
    val rowRDD = hFileAsRowRDD(path, conf)

    // Create a configuration object to use for saving
    @transient val outConfig = ConfigurationUtil.getOutputConfiguration(tableName, Seq[String](), zkUrl, Some(conf))

    // Retrieve the zookeeper URL
    val zkUrlFinal = ConfigurationUtil.getZookeeperURL(outConfig)

    // Retrieve the schema from table using PhoenixRelation
    val phoenixRDD = new PhoenixRDD(sqlContext.sparkContext, tableName, Seq(), None, zkUrl, new Configuration())
    val columnsList = PhoenixConfigurationUtil.getSelectColumnMetadataList(phoenixRDD.getPhoenixConfiguration).asScala.toList

    // Lookup the Spark catalyst types from the Phoenix schema
    val structFields = columnsList.map(ci => {
      val dataType = phoenixRDD.phoenixTypeToCatalystType(ci)
      StructField(ci.getDisplayName, dataType)
    }).toArray
    val structType = new StructType(structFields)

    // Create DataFrame using rowRDD, fieldsMapping and structType
    rowRDDAsDataFrame(rowRDD, fieldsMapping, columnsList, structType)

    // rowRDDAsDataFrame(rowRDD, fieldsMapping, structType)
  }

  def hFileAsDataFrameUsingStructType(path: String, fieldsMapping: Map[String, String], structType: StructType,
    conf: Configuration = new Configuration): DataFrame = {
    // Create a tuple with rowRDD
    val rowRDD = hFileAsRowRDD(path, conf)

    // Create DataFrame using rowRDD, fieldsMapping and structType
    rowRDDAsDataFrame(rowRDD, fieldsMapping, structType)
  }

  def rowRDDAsDataFrame(rowRDD: RDD[(String, String, String)], fieldsMapping: Map[String, String], columnsList: List[ColumnInfo], structType: StructType): DataFrame = {
    // get the schemaRDD from rowRDD
    val schemaRDD = getSchemaRDD(rowRDD, fieldsMapping, columnsList)

    // Create DataFrame using fullRowRDD and schema
    sqlContext.createDataFrame(schemaRDD, structType)
  }

  def rowRDDAsDataFrame(rowRDD: RDD[(String, String, String)], fieldsMapping: Map[String, String], structType: StructType): DataFrame = {
    // get the schemaRDD from rowRDD
    val schemaRDD = getSchemaRDD(rowRDD, fieldsMapping, structType)

    // Create DataFrame using fullRowRDD and schema
    sqlContext.createDataFrame(schemaRDD, structType)
  }

  def getSchemaRDD(rowRDD: RDD[(String, String, String)], fieldsMapping: Map[String, String], columnsList: List[ColumnInfo]): RDD[Row] = {
    // validate the schema
    var _fieldsMapping = Map[String, String]()
    if (fieldsMapping == null) {
      // Retrieve column names from rowRDD
      val columnNames = SchemaUtil.getEscapedArgument("rowkey") +: rowRDD.groupBy(x => x._2).map(x => x._1).collect()
      _fieldsMapping = columnNames.map { x => (x, x) }.toMap
    } else {
      _fieldsMapping = fieldsMapping
    }

    if (_fieldsMapping.size != columnsList.size) {
      val message = "columns in table size does not match with fields mapping columns size"
      throw new Exception(message)
    }

    val fvalues = _fieldsMapping.values.map { x => SchemaUtil.getUnEscapedFullColumnName(x) }.toList.sorted
    val cvalues = columnsList.map { x => SchemaUtil.getUnEscapedFullColumnName(x.getColumnName) }.toList.sorted
    if (fvalues.diff(cvalues).size != 0) {
      val message = "columns in table does not match with fields mapping columns"
      throw new Exception(message)
    }

    // Create a list with column name & ColumnInfo
    val fieldsList = columnsList.map { x => (x.getDisplayName, x) }
    val sfields = sqlContext.sparkContext.broadcast(fieldsList)

    // Create schemaRDD using rowRDD and sfields
    val schemaRDD = rowRDD.groupBy(x => x._1).map(row => {
      val rowKey = ("rowkey", row._1)
      val colList = row._2.map(t => (t._2, t._3)).toSeq
      val rowMap = (rowKey +: colList).toSeq
      val fieldsMap = sfields.value.toSeq

      // Prepare Row Sequence
      val rowData = fieldsMap.zip(rowMap).zipWithIndex.map {
        case ((c, v), i) => {
          phoenixTypeToScalaType(c._2, v._2)
        }
      }
      // Convert Row Sequence to Row Object
      Row.fromSeq(rowData)
    })

    schemaRDD
  }

  def getSchemaRDD(rowRDD: RDD[(String, String, String)], fieldsMapping: Map[String, String], structType: StructType): RDD[Row] = {
    // validate the schema
    var _fieldsMapping = Map[String, String]()
    if (fieldsMapping == null) {
      // Retrieve column names from rowRDD
      val columnNames = SchemaUtil.getEscapedArgument("rowkey") +: rowRDD.groupBy(x => x._2).map(x => x._1).collect()
      _fieldsMapping = columnNames.map { x => (x, x) }.toMap
    } else {
      _fieldsMapping = fieldsMapping
    }

    val fields = structType.fields
    val columnsList = fields.map { x => (x.name, x.dataType) }.toMap

    if (_fieldsMapping.size != columnsList.size) {
      val message = "columns size in structType does not match with columns size in fields mapping"
      throw new Exception(message)
    }

    val fvalues = _fieldsMapping.values.map { x => SchemaUtil.getUnEscapedFullColumnName(x) }.toList.sorted
    val cvalues = columnsList.map { x => SchemaUtil.getUnEscapedFullColumnName(x._1) }.toList.sorted
    if (fvalues.diff(cvalues).size != 0) {
      val message = s"column names in structType $cvalues does not match with column names in fields mapping $fvalues"
      throw new Exception(message)
    }

    // Create a list with column name & column data type
    val fieldsList = fields.map { x => (x.name, x.dataType) }.toSeq
    val sfields = sqlContext.sparkContext.broadcast(fieldsList)

    // Create schemaRDD using rowRDD and sfields
    // TODO: small letters bug
    val schemaRDD = rowRDD.groupBy(x => x._1).map(row => {
      val rowKey = (SchemaUtil.getEscapedArgument("rowkey"), row._1)
      val colList = row._2.map(t => (t._2, t._3)).toSeq
      val rowMap = (rowKey +: colList).toSeq
      val fieldsMap = sfields.value.toSeq

      // Prepare Row Sequence
      val rowData = fieldsMap.zip(rowMap).zipWithIndex.map {
        case ((v, c), i) => {
          catalystTypeToScalaType(v._2, c._2)
        }
      }
      // Convert Row Sequence to Row Object
      Row.fromSeq(rowData)
    })

    schemaRDD
  }

  // prepare tuple with (rowkey, columnfamily.qualifier, value)
  def convertToRow(data: Iterator[(ImmutableBytesWritable, KeyValue)]): Iterator[(String, String, String)] = {
    data.map(row => {
      val keyvalue = row._2
      val rowkey = Bytes.toStringBinary(keyvalue.getRow())
      val family = Bytes.toStringBinary(keyvalue.getFamily())
      val qualifier = Bytes.toStringBinary(keyvalue.getQualifier())
      val value = Bytes.toStringBinary(keyvalue.getValue)
      val column = SchemaUtil.getEscapedFullColumnName(family + "." + qualifier)
      (rowkey, column, value)
    })
  }

  // Lookup table for Phoenix types to Spark catalyst types
  def phoenixTypeToScalaType(columnInfo: ColumnInfo, value: String): Any = columnInfo.getPDataType match {
    case t if t.isInstanceOf[PVarchar] || t.isInstanceOf[PChar] => PVarchar.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PVarbinary] || t.isInstanceOf[PBinary] => PVarbinary.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PLong] || t.isInstanceOf[PUnsignedLong] => PLong.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PInteger] || t.isInstanceOf[PUnsignedInt] => PInteger.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PSmallint] || t.isInstanceOf[PUnsignedSmallint] => PSmallint.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PTinyint] || t.isInstanceOf[PUnsignedTinyint] => PTinyint.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PFloat] || t.isInstanceOf[PUnsignedFloat] => PFloat.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PDouble] || t.isInstanceOf[PUnsignedDouble] => PDouble.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PBoolean] => PBoolean.INSTANCE.toObject(value)
    // Use Spark system default precision for now (explicit to work with < 1.5)
    case t if t.isInstanceOf[PDecimal] => PDecimal.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PTimestamp] || t.isInstanceOf[PUnsignedTimestamp] => PTimestamp.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PTime] || t.isInstanceOf[PUnsignedTime] => PTime.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PDate] || t.isInstanceOf[PUnsignedDate] => PDate.INSTANCE.toObject(value)

    case t if t.isInstanceOf[PVarcharArray] || t.isInstanceOf[PCharArray] => PVarcharArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PVarbinaryArray] || t.isInstanceOf[PBinaryArray] => PVarbinaryArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PLongArray] || t.isInstanceOf[PUnsignedLongArray] => PLongArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PIntegerArray] || t.isInstanceOf[PUnsignedIntArray] => PIntegerArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PSmallintArray] || t.isInstanceOf[PUnsignedSmallintArray] => PSmallintArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PTinyintArray] || t.isInstanceOf[PUnsignedTinyintArray] => PTinyintArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PFloatArray] || t.isInstanceOf[PUnsignedFloatArray] => PFloatArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PDoubleArray] || t.isInstanceOf[PUnsignedDoubleArray] => PDoubleArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PBooleanArray] => PBooleanArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PDecimalArray] => PDecimalArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PTimestampArray] || t.isInstanceOf[PUnsignedTimestampArray] => PTimestampArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PDateArray] || t.isInstanceOf[PUnsignedDateArray] => PDateArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[PTimeArray] || t.isInstanceOf[PUnsignedTimeArray] => PTimeArray.INSTANCE.toObject(value)
  }

  // Lookup table for  Spark catalyst types to Scala types
  // TODO: Array Types are not completed..
  def catalystTypeToScalaType(datatype: DataType, value: String): Any = datatype match {
    case t if t.isInstanceOf[StringType] => PVarchar.INSTANCE.toObject(value)
    case t if t.isInstanceOf[BinaryType] => PVarbinary.INSTANCE.toObject(value)
    case t if t.isInstanceOf[LongType] => PLong.INSTANCE.toObject(value)
    case t if t.isInstanceOf[IntegerType] => PInteger.INSTANCE.toObject(value)
    case t if t.isInstanceOf[ShortType] => PSmallint.INSTANCE.toObject(value)
    case t if t.isInstanceOf[ByteType] => PTinyint.INSTANCE.toObject(value)
    case t if t.isInstanceOf[FloatType] => PFloat.INSTANCE.toObject(value)
    case t if t.isInstanceOf[DoubleType] => PDouble.INSTANCE.toObject(value)
    case t if t.isInstanceOf[BooleanType] => PBoolean.INSTANCE.toObject(value)
    case t if t.isInstanceOf[DecimalType] => PDecimal.INSTANCE.toObject(value)
    case t if t.isInstanceOf[TimestampType] => PTimestamp.INSTANCE.toObject(value)
    case t if t.isInstanceOf[DateType] => PDate.INSTANCE.toObject(value)
    case t if t.isInstanceOf[ArrayType] => catalystArrayTypeToScalaType(datatype, value)
  }

  def catalystArrayTypeToScalaType(datatype: DataType, value: String): Any = datatype match {
    case t if t.isInstanceOf[StringType] => PVarcharArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[BinaryType] => PVarbinaryArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[LongType] => PLongArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[IntegerType] => PIntegerArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[ShortType] => PSmallint.INSTANCE.toObject(value)
    case t if t.isInstanceOf[ByteType] => PTinyint.INSTANCE.toObject(value)
    case t if t.isInstanceOf[FloatType] => PFloatArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[DoubleType] => PDoubleArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[BooleanType] => PBooleanArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[DecimalType] => PDecimalArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[TimestampType] => PTimestampArray.INSTANCE.toObject(value)
    case t if t.isInstanceOf[DateType] => PDateArray.INSTANCE.toObject(value)
  }

}












