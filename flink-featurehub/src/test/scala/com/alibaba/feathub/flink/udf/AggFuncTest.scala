package com.alibaba.feathub.flink.udf

import com.alibaba.feathub.flink.udf.aggregation._
import org.apache.flink.table.api.DataTypes

object AggFuncTest {
  def main(args: Array[String]): Unit = {
    println("=== Flink FeatureHub 聚合函数测试 ===")
    
    testCountAggFunc()
    testSumAggFunc()
    testAvgAggFunc() 
    testMinMaxAggFunc()
    
    println("=== 所有测试完成 ===")
  }
  
  def testCountAggFunc(): Unit = {
    println("\n--- 测试 CountAggFunc ---")
    
    val countFunc = new CountAggFunc()
    val acc = countFunc.createAccumulator()
    
    println(s"初始累加器: count = ${countFunc.getResult(acc)}")
    
    countFunc.add(acc, "value1", System.currentTimeMillis())
    countFunc.add(acc, "value2", System.currentTimeMillis())
    countFunc.add(acc, "value3", System.currentTimeMillis())
    
    println(s"添加3个值后: count = ${countFunc.getResult(acc)}")
    
    countFunc.retract(acc, "value1")
    println(s"撤回1个值后: count = ${countFunc.getResult(acc)}")
    
    val acc2 = countFunc.createAccumulator()
    countFunc.add(acc2, "value4", System.currentTimeMillis())
    countFunc.add(acc2, "value5", System.currentTimeMillis())
    
    countFunc.merge(acc, acc2)
    println(s"合并累加器后: count = ${countFunc.getResult(acc)}")
    
    println(s"结果类型: ${countFunc.getResultDatatype()}")
  }
  
  def testSumAggFunc(): Unit = {
    println("\n--- 测试 SumAggFunc ---")
    
    // 测试 IntSumAggFunc
    println("测试 IntSumAggFunc:")
    val intSumFunc = new SumAggFunc.IntSumAggFunc()
    val intAcc = intSumFunc.createAccumulator()
    
    println(s"初始累加器: sum = ${intSumFunc.getResult(intAcc)}")
    
    intSumFunc.add(intAcc, 10, System.currentTimeMillis())
    intSumFunc.add(intAcc, 20, System.currentTimeMillis())
    intSumFunc.add(intAcc, 30, System.currentTimeMillis())
    
    println(s"添加10,20,30后: sum = ${intSumFunc.getResult(intAcc)}")
    
    intSumFunc.retract(intAcc, 10)
    println(s"撤回10后: sum = ${intSumFunc.getResult(intAcc)}")
    
    // 测试 DoubleSumAggFunc
    println("\n测试 DoubleSumAggFunc:")
    val doubleSumFunc = new SumAggFunc.DoubleSumAggFunc()
    val doubleAcc = doubleSumFunc.createAccumulator()
    
    doubleSumFunc.add(doubleAcc, 1.5, System.currentTimeMillis())
    doubleSumFunc.add(doubleAcc, 2.3, System.currentTimeMillis())
    doubleSumFunc.add(doubleAcc, 3.2, System.currentTimeMillis())
    
    println(s"添加1.5,2.3,3.2后: sum = ${doubleSumFunc.getResult(doubleAcc)}")
    println(s"结果类型: ${doubleSumFunc.getResultDatatype()}")
  }
  
  def testAvgAggFunc(): Unit = {
    println("\n--- 测试 AvgAggFunc ---")
    
    val intSumFunc = new SumAggFunc.IntSumAggFunc()
    val avgFunc = new AvgAggFunc[Integer](intSumFunc)
    val acc = avgFunc.createAccumulator()
    
    println(s"初始累加器: avg = ${avgFunc.getResult(acc)}")
    
    avgFunc.add(acc, 10, System.currentTimeMillis())
    avgFunc.add(acc, 20, System.currentTimeMillis())
    avgFunc.add(acc, 30, System.currentTimeMillis())
    
    println(s"添加10,20,30后: avg = ${avgFunc.getResult(acc)}")
    
    avgFunc.add(acc, 40, System.currentTimeMillis())
    println(s"添加40后: avg = ${avgFunc.getResult(acc)}")
    
    avgFunc.retract(acc, 10)
    println(s"撤回10后: avg = ${avgFunc.getResult(acc)}")
    
    println(s"结果类型: ${avgFunc.getResultDatatype()}")
  }
  
  def testMinMaxAggFunc(): Unit = {
    println("\n--- 测试 MinMaxAggFunc ---")
    
    // 测试 Min
    println("测试 MinAggFunc:")
    val minFunc = new MinMaxAggFunc[Integer](DataTypes.INT(), true)
    val minAcc = minFunc.createAccumulator()
    
    println(s"初始累加器: min = ${minFunc.getResult(minAcc)}")
    
    minFunc.add(minAcc, 30, System.currentTimeMillis())
    minFunc.add(minAcc, 10, System.currentTimeMillis())
    minFunc.add(minAcc, 20, System.currentTimeMillis())
    minFunc.add(minAcc, 10, System.currentTimeMillis()) // 重复值
    
    println(s"添加30,10,20,10后: min = ${minFunc.getResult(minAcc)}")
    
    minFunc.retract(minAcc, 10)
    println(s"撤回一个10后: min = ${minFunc.getResult(minAcc)}")
    
    minFunc.retract(minAcc, 10)
    println(s"撤回另一个10后: min = ${minFunc.getResult(minAcc)}")
    
    // 测试 Max
    println("\n测试 MaxAggFunc:")
    val maxFunc = new MinMaxAggFunc[Integer](DataTypes.INT(), false)
    val maxAcc = maxFunc.createAccumulator()
    
    maxFunc.add(maxAcc, 30, System.currentTimeMillis())
    maxFunc.add(maxAcc, 10, System.currentTimeMillis())
    maxFunc.add(maxAcc, 50, System.currentTimeMillis())
    maxFunc.add(maxAcc, 20, System.currentTimeMillis())
    
    println(s"添加30,10,50,20后: max = ${maxFunc.getResult(maxAcc)}")
    
    maxFunc.retract(maxAcc, 50)
    println(s"撤回50后: max = ${maxFunc.getResult(maxAcc)}")
    
    println(s"结果类型: ${maxFunc.getResultDatatype()}")
  }
}