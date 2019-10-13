package spark.product

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * 注册为group_concat_distinct
  */
class GroupConcatDistinctUDAF extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(Array[StructField](StructField("cityInfo", StringType, nullable = true)))
  override def bufferSchema: StructType = StructType(Array[StructField](StructField("bufferCityInfo", StringType, nullable = true)))
  override def dataType: DataType = StringType
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, "")
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo: String = buffer.getString(0)
    val cityInfo: String = input.getString(0)

    if (!bufferCityInfo.contains(cityInfo))
      if ("".equals(bufferCityInfo))
        bufferCityInfo += cityInfo
      else bufferCityInfo += "," + cityInfo

    buffer.update(0, bufferCityInfo)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var bufferCityInfo1: String = buffer1.getString(0)
    val bufferCityInfo2: String = buffer2.getString(0)

    bufferCityInfo2.split(",").foreach(cityInfo => {
      if (!bufferCityInfo1.contains(cityInfo))
        if ("".equals(bufferCityInfo1))
          bufferCityInfo1 += cityInfo
        else
          bufferCityInfo1 += "," + cityInfo
    })
    buffer1.update(0, bufferCityInfo1)
  }

  override def evaluate(row: Row): Any = {
    row.getString(0)
  }

}
