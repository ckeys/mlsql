package streaming.dsl.mmlib.fe

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.{ETMethod, PREDICT}

class SQLDataSummary(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {
  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val summary = df.describe()
    val quantileNum = df.stat.approxQuantile(df.columns, Array(0.25, 0.5, 0.75), 0.05)
    val transpose = quantileNum.transpose
    var transformedRows = transpose.map(row => {
      var newRow = Seq("PLACEHOLDER")
      row.foreach(subRow => {
        newRow = newRow :+ subRow.toString
      })
      newRow
    }
    )

    transformedRows = transformedRows.updated(0, transformedRows(0).updated(0, "25%"))
    transformedRows = transformedRows.updated(1, transformedRows(1).updated(0, "50%"))
    transformedRows = transformedRows.updated(2, transformedRows(2).updated(0, "75%"))
    val appendRows = transformedRows.map(Row.fromSeq(_))
    val spark = df.sparkSession
    val unionedTable = spark.createDataFrame(spark.sparkContext.parallelize(appendRows, 1), summary.schema)
    return summary.union(unionedTable)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_data_summary_operator__"),
      OperateType.SELECT,
      Option("select"),
      TableType.SYSTEM)

    val context = ScriptSQLExec.contextGetOrForTest()
    context.execListener.getTableAuth match {
      case Some(tableAuth) =>
        tableAuth.auth(List(vtable))
      case None =>
        List(TableAuthResult(granted = true, ""))
    }
  }


}
