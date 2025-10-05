package com.alibaba.feathub.flink.udf

import com.alibaba.feathub.flink.udf.aggregation._
import com.alibaba.feathub.flink.udf.processfunction._
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.table.api.DataTypes
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

import java.time.Instant
import scala.collection.JavaConverters._

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    println("=== Flink FeatureHub ProcessFunction 测试 ===")
    
    testWindowUtils()
    testAggregationFieldsDescriptor()
    testSlidingWindowExpiredRowHandler()
    testGlobalWindowKeyedProcessFunction()
    
    println("=== ProcessFunction 测试完成 ===")
  }
  
  def testWindowUtils(): Unit = {
    println("\n--- 测试 WindowUtils ---")
    
    val aggDescriptor = AggregationFieldsDescriptor.builder()
      .addField("count_field", DataTypes.BIGINT(), DataTypes.BIGINT(), 5000L, null, null, "COUNT")
      .addField("sum_field", DataTypes.INT(), DataTypes.INT(), 5000L, null, null, "SUM")
      .build()
    
    val row1 = Row.withNames()
    row1.setField("count_field", 5L)
    row1.setField("sum_field", 100)
    row1.setField("other_field", "test")
    
    val row2 = Row.withNames()
    row2.setField("count_field", 5L) 
    row2.setField("sum_field", 100)
    row2.setField("other_field", "different")
    
    val row3 = Row.withNames()
    row3.setField("count_field", 3L)
    row3.setField("sum_field", 100)
    row3.setField("other_field", "test")
    
    // WindowUtils 是包私有的，我们通过反射来测试其功能
    import java.lang.reflect.Method
    val windowUtilsClass = Class.forName("com.alibaba.feathub.flink.udf.processfunction.WindowUtils")
    val method: Method = windowUtilsClass.getDeclaredMethod("hasEqualAggregationResult", 
      classOf[AggregationFieldsDescriptor], classOf[Row], classOf[Row])
    method.setAccessible(true)
    
    val isEqual1 = method.invoke(null, aggDescriptor, row1, row2).asInstanceOf[Boolean]
    val isEqual2 = method.invoke(null, aggDescriptor, row1, row3).asInstanceOf[Boolean]
    val isEqual3 = method.invoke(null, aggDescriptor, null, null).asInstanceOf[Boolean]
    val isEqual4 = method.invoke(null, aggDescriptor, row1, null).asInstanceOf[Boolean]
    
    println(s"相同聚合结果的行比较 (应该为true): $isEqual1")
    println(s"不同聚合结果的行比较 (应该为false): $isEqual2") 
    println(s"两个null行比较 (应该为true): $isEqual3")
    println(s"一个null一个非null行比较 (应该为false): $isEqual4")
  }
  
  def testAggregationFieldsDescriptor(): Unit = {
    println("\n--- 测试 AggregationFieldsDescriptor ---")
    
    val descriptor = AggregationFieldsDescriptor.builder()
      .addField("count_1h", DataTypes.BIGINT(), DataTypes.BIGINT(), 3600000L, null, null, "COUNT")
      .addField("sum_30m", DataTypes.INT(), DataTypes.INT(), 1800000L, null, null, "SUM") 
      .addField("avg_2h", DataTypes.DOUBLE(), DataTypes.DOUBLE(), 7200000L, null, null, "AVG")
      .build()
    
    println(s"聚合字段数量: ${descriptor.getAggFieldDescriptors.size()}")
    println(s"最大窗口大小 (ms): ${descriptor.getMaxWindowSizeMs}")
    
    descriptor.getAggFieldDescriptors.asScala.zipWithIndex.foreach { case (field, idx) =>
      println(s"字段 $idx: ${field.fieldName}, 窗口大小: ${field.windowSizeMs}ms, 聚合函数类型: ${field.aggFunc.getClass.getSimpleName}")
      val fieldIdx = descriptor.getAggFieldIdx(field)
      println(s"  字段索引: $fieldIdx")
    }
  }
  
  def testSlidingWindowExpiredRowHandler(): Unit = {
    println("\n--- 测试 SlidingWindowExpiredRowHandler ---")
    
    val handler = new SlidingWindowExpiredRowHandler {
      override def handleExpiredRow(out: Collector[Row], expiredRow: Row, currentTimestamp: Long): Unit = {
        println(s"处理过期行: timestamp=$currentTimestamp")
        println(s"过期行内容: $expiredRow")
      }
    }
    
    val mockCollector = new Collector[Row] {
      override def collect(record: Row): Unit = {
        println(s"收集输出行: $record")
      }
      
      override def close(): Unit = {}
    }
    
    val expiredRow = Row.withNames()
    expiredRow.setField("key", "test_key")
    expiredRow.setField("value", 42)
    expiredRow.setField("timestamp", Instant.now())
    
    handler.handleExpiredRow(mockCollector, expiredRow, System.currentTimeMillis())
    
    println("过期行处理器测试完成")
  }
  
  def testGlobalWindowKeyedProcessFunction(): Unit = {
    println("\n--- 测试 GlobalWindowKeyedProcessFunction ---")
    
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    
    val aggDescriptor = AggregationFieldsDescriptor.builder()
      .addField("count_result", DataTypes.BIGINT(), DataTypes.BIGINT(), null, null, null, "COUNT")
      .addField("sum_result", DataTypes.INT(), DataTypes.INT(), null, null, null, "SUM")
      .build()
    
    val keyFieldNames = Array("key")
    val rowTimeFieldName = "timestamp"
    val resultTypeInfo = Types.ROW(
      Types.STRING,  // key
      Types.LONG,    // count_result  
      Types.INT,     // sum_result
      Types.GENERIC(classOf[Instant]) // timestamp
    )
    
    val processFunction = new GlobalWindowKeyedProcessFunction(
      keyFieldNames,
      rowTimeFieldName, 
      aggDescriptor,
      resultTypeInfo
    )
    
    println("GlobalWindowKeyedProcessFunction 创建成功")
    println(s"Key字段: ${keyFieldNames.mkString(", ")}")
    println(s"时间字段: $rowTimeFieldName")
    println(s"聚合字段数量: ${aggDescriptor.getAggFieldDescriptors.size()}")
    
    val dataStream: DataStream[Row] = env.addSource(new SourceFunction[Row] {
      override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
        for (i <- 1 to 5) {
          val row = Row.withNames()
          row.setField("key", "test_key")
          row.setField("value", i * 10)
          row.setField("timestamp", Instant.now())
          ctx.collect(row)
          Thread.sleep(100)
        }
      }
      
      override def cancel(): Unit = {}
    })
    
    import org.apache.flink.api.java.functions.KeySelector
    
    val keySelector = new KeySelector[Row, Row] {
      override def getKey(row: Row): Row = {
        val keyRow = Row.withNames()
        keyRow.setField("key", row.getFieldAs[String]("key"))
        keyRow
      }
    }
    
    val resultStream = dataStream
      .keyBy(keySelector, Types.ROW(Types.STRING))
      .process(processFunction)
    
    resultStream.print("GlobalWindow结果")
    
    println("GlobalWindowKeyedProcessFunction 流处理配置完成")
    println("(注意: 这是配置测试，实际运行需要调用 env.execute())")
  }
}