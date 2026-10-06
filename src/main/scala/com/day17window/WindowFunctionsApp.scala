package com.day17window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object WindowFunctionsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day17 Window Functions Practice")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // 1. READ STUDENT DATA
    // ============================================================

    val students = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/students.csv")

    println("\n========== STUDENT DATA ==========")
    students.show(false)

    // ============================================================
    // 2. ROW_NUMBER - TOP 3 STUDENTS PER COURSE
    // ============================================================

    val courseWindow = Window
      .partitionBy("course")
      .orderBy(col("score").desc, col("student_id"))

    val studentsWithRowNumber = students
      .withColumn("row_number", row_number().over(courseWindow))

    println("\n========== ROW_NUMBER PER COURSE ==========")

    studentsWithRowNumber
      .orderBy("course", "row_number")
      .show(false)

    println("\n========== TOP 3 STUDENTS PER COURSE ==========")

    studentsWithRowNumber
      .filter(col("row_number") <= 3)
      .orderBy("course", "row_number")
      .show(false)

    // ============================================================
    // 3. RANK
    // ============================================================

    val rankWindow = Window
      .partitionBy("course")
      .orderBy(col("score").desc)

    val rankedStudents = students
      .withColumn("rank", rank().over(rankWindow))

    println("\n========== RANK PER COURSE ==========")

    rankedStudents
      .orderBy("course", "rank", "student_id")
      .show(false)

    // ============================================================
    // 4. DENSE_RANK
    // ============================================================

    val denseRankedStudents = students
      .withColumn("dense_rank", dense_rank().over(rankWindow))

    println("\n========== DENSE_RANK PER COURSE ==========")

    denseRankedStudents
      .orderBy("course", "dense_rank", "student_id")
      .show(false)

    // ============================================================
    // 5. DEPARTMENT PARTITION
    // ============================================================

    val departmentWindow = Window
      .partitionBy("department")
      .orderBy(col("score").desc)

    val departmentRanking = students
      .withColumn(
        "department_rank",
        dense_rank().over(departmentWindow)
      )

    println("\n========== DEPARTMENT RANKING ==========")

    departmentRanking
      .orderBy("department", "department_rank")
      .show(false)

    // ============================================================
    // 6. READ POLICY DATA
    // ============================================================

    val policies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/policies.csv")

    println("\n========== POLICY DATA ==========")
    policies.show(false)

    // ============================================================
    // 7. LATEST POLICY PER CUSTOMER
    // ============================================================

    val customerWindow = Window
      .partitionBy("customer_id")
      .orderBy(col("policy_date").desc)

    val latestPolicies = policies
      .withColumn(
        "row_number",
        row_number().over(customerWindow)
      )

    println("\n========== LATEST POLICY PER CUSTOMER ==========")

    latestPolicies
      .filter(col("row_number") === 1)
      .orderBy("customer_id")
      .show(false)

    // ============================================================
    // 8. LAG - PREVIOUS POLICY PREMIUM
    // ============================================================

    val policyHistoryWindow = Window
      .partitionBy("customer_id")
      .orderBy("policy_date")

    val policyHistory = policies
      .withColumn(
        "previous_premium",
        lag("premium", 1).over(policyHistoryWindow)
      )

    println("\n========== LAG - PREVIOUS PREMIUM ==========")

    policyHistory
      .orderBy("customer_id", "policy_date")
      .show(false)

    // ============================================================
    // 9. LEAD - NEXT POLICY PREMIUM
    // ============================================================

    val policyWithNext = policies
      .withColumn(
        "next_premium",
        lead("premium", 1).over(policyHistoryWindow)
      )

    println("\n========== LEAD - NEXT PREMIUM ==========")

    policyWithNext
      .orderBy("customer_id", "policy_date")
      .show(false)

    // ============================================================
    // 10. PREMIUM CHANGE
    // ============================================================

    val premiumChanges = policyHistory
      .withColumn(
        "premium_change",
        col("premium") - col("previous_premium")
      )

    println("\n========== PREMIUM CHANGE ==========")

    premiumChanges
      .orderBy("customer_id", "policy_date")
      .show(false)

    // ============================================================
    // 11. ROUTE DATA
    // ============================================================

    val routes = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/routes.csv")

    println("\n========== ROUTE DATA ==========")
    routes.show(false)

    // ============================================================
    // 12. ROUTE RANKING
    // ============================================================

    val routeWindow = Window
      .partitionBy("route")
      .orderBy(col("revenue").desc)

    val routeRanking = routes
      .withColumn(
        "route_rank",
        dense_rank().over(routeWindow)
      )

    println("\n========== ROUTE REVENUE RANKING ==========")

    routeRanking
      .orderBy("route", "route_rank")
      .show(false)

    // ============================================================
    // 13. TOP ROUTE PER DEPARTMENT
    // ============================================================

    val departmentRouteWindow = Window
      .partitionBy("department")
      .orderBy(col("revenue").desc)

    val topRoutePerDepartment = routes
      .withColumn(
        "department_rank",
        row_number().over(departmentRouteWindow)
      )
      .filter(col("department_rank") === 1)

    println("\n========== TOP ROUTE PER DEPARTMENT ==========")

    topRoutePerDepartment
      .show(false)

    println("\n========== DAY 17 COMPLETED ==========")

    spark.stop()
  }
}
