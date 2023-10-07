package com.sparkTutorial.sparkSql.join;


import com.sparkTutorial.sparkSql.EmployeeCustomJoin;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Neha Priya on 02-10-2019.
 */
public class CustomJoin {
    public static void main(String[] args) throws Exception {
        SparkSession mySparkSession = SparkSession.builder()
                .master("local")
                .appName("Spark-SQL Joins ")
                .getOrCreate();


        EmployeeCustomJoin emp = new EmployeeCustomJoin("Test", 31);
        EmployeeCustomJoin emp1 = new EmployeeCustomJoin("Rest", 33);
        EmployeeCustomJoin emp2 = new EmployeeCustomJoin("Nest", 34);

        List<EmployeeCustomJoin> listEmp = new ArrayList<EmployeeCustomJoin>();
        listEmp.add(emp);
        listEmp.add(emp1);
        listEmp.add(emp2);

        Dataset<Row> employeeDF = mySparkSession.createDataset(listEmp, Encoders.bean(EmployeeCustomJoin.class)).
                toDF();
        employeeDF.createOrReplaceTempView("employees");

        Department dept = new Department(1,"Sales");
        List<Department> listdept = new ArrayList<Department>();
                listdept.add(new Department(31, "Sales"));
                listdept.add(new Department(33, "Engineering"));
                listdept.add(new Department(34, "Finance"));
                listdept.add(new Department(35, "Marketing"));

        Dataset<Row> deptDF = mySparkSession.createDataset(listdept, Encoders.bean(Department.class)).
                toDF();
        deptDF.createOrReplaceTempView("departments");

        // define the join expression of equality comparison
        Column joinExpression = employeeDF.col("dept_no").startsWith(deptDF.col("id"));

        // perform the join
        employeeDF.join(deptDF,joinExpression,"inner").show();

       // no need to specify the join type since "inner" is the default
        employeeDF.join(deptDF, joinExpression).show();

        // using SQL
        mySparkSession.sql("select * from employees JOIN departments on dept_no == id").show();
    }
}
