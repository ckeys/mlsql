export MLSQL_HOME=/Users/yonghui.huang/execTest/mlsql-engine_2.4-2.1.0
export ENABLE_CHINESE_ANALYZER=false
./dev/make-distribution.sh
cp mlsql-engine_2.4-2.1.0.tar.gz /Users/yonghui.huang/execTest/
tar zxvf /Users/yonghui.huang/execTest/mlsql-engine_2.4-2.1.0.tar.gz -C /Users/yonghui.huang/execTest/
nohup /Users/yonghui.huang/execTest/mlsql-engine_2.4-2.1.0/bin/start-local.sh 2>&1 > /Users/yonghui.huang/execTest/mlsql-engine_2.4-2.1.0/local_mlsql.log &
