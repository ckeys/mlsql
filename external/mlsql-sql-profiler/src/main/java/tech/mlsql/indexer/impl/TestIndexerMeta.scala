package tech.mlsql.indexer.impl

import tech.mlsql.indexer.{MLSQLIndexerMeta, MlsqlIndexer, MlsqlOriTable}

/**
 * 21/12/2020 WilliamZhu(allwefantasy@gmail.com)
 */
class TestIndexerMeta extends MLSQLIndexerMeta {
  override def fetchIndexers(tableNames: List[MlsqlOriTable], options: Map[String, String]): Map[MlsqlOriTable, MlsqlIndexer] = {
    Map(
      MlsqlOriTable(
        "newtable", "delta", "tmp.newtable", ""
      ) -> MlsqlIndexer(
        name = "xxxxx",
        oriFormat = "delta",
        oriPath = "tmp.newtable",
        oriStorageName = "",
        format = "delta",
        path = "_mlsql_indexer_.delta_tmp_newtable",
        storageName = "",
        status = 0,
        lastStatus = 0,
        lastFailMsg = "",
        lastExecuteTime = 0,
        syncInterval = 0,
        content = "",
        indexerConfig = "",
        lastJobId = "",
        indexerType = "nested"
      )
    )

  }
}
