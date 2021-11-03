package streaming.dsl.mmlib.fe

import org.apache.spark.ml.Estimator
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.{DB_DEFAULT, MLSQLTable, OperateType, TableAuthResult, TableType}
import streaming.dsl.mmlib.SQLAlg
import streaming.dsl.mmlib.algs.{Functions, MllibFunctions}
import streaming.dsl.mmlib.algs.param.BaseParams
import tech.mlsql.dsl.auth.ETAuth
import tech.mlsql.dsl.auth.dsl.mmlib.ETMethod.ETMethod

class SQLMissingValueProcess(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseParams with ETAuth {

  def this() = this(BaseParams.randomUID())

  private def randomForestRegressor(df: DataFrame, params: Map[String, String]): Unit = {
//    val regressor = new RandomForestRegressor()
//    val processColumns = params.getOrElse("processColumns", "")
//    val processColArrays = processColumns.split(",")
//    configureModel(regressor, params)
//    val fullColumns = df.columns
//    processColArrays.map(col => {
//      val unknown_data = df.where(s"$col===null or $col===\"\"")
//      val known_data = df.where(s"$col!==null and $col!==\"\"")
//      val features =
//      regressor.setLabelCol(col).setFeaturesCol()
//
//    })
//
//    val train_df = df.
//    val model = regressor.fit(df)


  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    return null
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = ???

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = ???

  override def auth(etMethod: ETMethod, path: String, params: Map[String, String]): List[TableAuthResult] = {
    val vtable = MLSQLTable(
      Option(DB_DEFAULT.MLSQL_SYSTEM.toString),
      Option("__fe_missingvalue_process_operator__"),
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
