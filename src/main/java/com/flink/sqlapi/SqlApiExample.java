package main.java.com.flink.sqlapi;

import org.apache.flink.table.api.Table;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
//import org.apache.flink.table.api.java.BatchTableEnvironment;
//import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

public class SqlApiExample {

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

    String sql = "SELECT `month`, SUM(profit) AS sum1 FROM CatalogTable WHERE category = 'Category5'" +
      " GROUP BY `month` ORDER BY sum1";
    Table order20 = tableEnv.sqlQuery(sql);

    // BatchTableEnvironment required to convert Table to Dataset
    BatchTableEnvironment bTableEnv = BatchTableEnvironment.create(env);
    DataSet < Row1 > order20Set = bTableEnv.toDataSet(order20, Row1.class);

    order20Set.writeAsText("/home/jivesh/table_sql");
    env.execute("SQL API Example");
  }

  public static class Row1 {
    public String month;
    public Integer sum1;

    public Row1() {}

    public String toString() {
      return month + "," + sum1;
    }
  }

}