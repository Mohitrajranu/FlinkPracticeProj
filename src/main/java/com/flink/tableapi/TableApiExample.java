package main.java.com.flink.tableapi;

import org.apache.flink.table.api.Table;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

public class TableApiExample {

  public static void main(String[] args) throws Exception {
    EnvironmentSettings settings = EnvironmentSettings
      .newInstance()
      //.inStreamingMode()
      .inBatchMode()
      .build();

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    final String tableDDL = "CREATE TEMPORARY TABLE CatalogTable (" +
      "date STRING, " +
      "month STRING, " +
      "category STRING, " +
      "product STRING, " +
      "profit INT " +
      ") WITH (" +
      "'connector' = 'filesystem', " +
      "'path' = 'file:///home/jivesh/avg', " +
      "'format' = 'csv'" +
      ")";

    tableEnv.executeSql(tableDDL);

    Table catalog = tableEnv.from("CatalogTable");

    /* querying with Table API */
    Table order20 = catalog
      .filter($("category").isEqual("Category5"))
      .groupBy($("month"))
      .select($("month"), $("profit").sum().as("sum"))
      .orderBy($("sum"));

    // BatchTableEnvironment required to convert Table to Dataset
    BatchTableEnvironment bTableEnv = BatchTableEnvironment.create(env);
    DataSet < Row1 > order20Set = bTableEnv.toDataSet(order20, Row1.class);

    order20Set.writeAsText("/home/jivesh/table1");
    env.execute("State");
  }

  public static class Row1 {
    public String month;
    public Integer sum;

    public Row1() {}

    public String toString() {
      return month + "," + sum;
    }
  }

}