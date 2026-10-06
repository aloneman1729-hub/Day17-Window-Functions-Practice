# Day 17 - Spark Window Functions Practice

A Scala and Apache Spark project demonstrating the use of **Window Functions** for ranking, historical analysis, and latest-record selection.

## Technologies

- Scala 2.12.18
- Apache Spark 3.5.1
- Spark SQL
- SBT 1.9.9

## Project Structure

```text
Day17-Window-Functions-Practice/
├── build.sbt
├── project/
│   └── build.properties
├── src/
│   └── main/
│       ├── scala/
│       │   └── com/day17window/
│       │       └── WindowFunctionsApp.scala
│       └── resources/
│           └── data/
│               ├── students.csv
│               ├── policies.csv
│               └── routes.csv
└── .gitignore
Window Functions Covered
1. ROW_NUMBER

Used to assign a unique sequential number to records within each partition.

Example:

row_number().over(courseWindow)

The project uses row_number to identify the top 3 students in each course.

2. RANK

Used to assign rankings while giving the same rank to tied values.

rank().over(rankWindow)

For example, if two students have the same score:

Score    Rank
94       1
89       2
89       2
80       4
3. DENSE_RANK

Similar to rank, but it does not leave gaps after ties.

dense_rank().over(rankWindow)

Example:

Score    Dense Rank
94       1
89       2
89       2
80       3
4. PARTITION BY

Window partitions are used to perform calculations independently for groups such as:

Course
Department
Customer
Route

Example:

Window
  .partitionBy("course")
  .orderBy(col("score").desc)
5. Latest Record Per Customer

The project identifies the latest insurance policy for every customer using:

Window
  .partitionBy("customer_id")
  .orderBy(col("policy_date").desc)

Combined with row_number, the record with:

row_number = 1

represents the latest policy.

6. LAG

lag retrieves a previous record within a window.

lag("premium", 1).over(policyHistoryWindow)

It is used to compare a customer's current insurance premium with their previous premium.

7. LEAD

lead retrieves the next record within a window.

lead("premium", 1).over(policyHistoryWindow)

It is used to identify the next premium in a customer's policy history.

8. Premium Change

The project calculates the change between the current and previous premium:

col("premium") - col("previous_premium")
Business Scenarios
Student Ranking

The student dataset demonstrates:

Top 3 students per course
Row numbering
Ranking with ties
Dense ranking
Department-level ranking
Insurance Policy Analysis

The policy dataset demonstrates:

Latest policy per customer
Previous premium using LAG
Next premium using LEAD
Premium change calculation
Route Revenue Analysis

The route dataset demonstrates:

Revenue ranking by route
Ranking within departments
Finding the top route per department
How to Run

From the project directory:

sbt run

The application runs locally using:

.master("local[*]")
Expected Output

The application prints results for:

Student data
ROW_NUMBER per course
Top 3 students per course
RANK per course
DENSE_RANK per course
Department ranking
Policy data
Latest policy per customer
Previous premium using LAG
Next premium using LEAD
Premium changes
Route data
Route revenue ranking
Top route per department

At the end of a successful execution:

========== DAY 17 COMPLETED ==========
Learning Objective

The main objective of this project is to understand how Apache Spark Window Functions can be used for:

Ranking records
Handling ties
Selecting top-N records
Comparing current and previous records
Comparing current and next records
Finding the latest record within a group
Performing partition-based analytics
Author

aloneman1729-hub

