
TSDB_TABLE=${TSDB_TABLE-'tsdb'}
UID_TABLE=${UID_TABLE-'tsdb-uid'}
TREE_TABLE=${TREE_TABLE-'tsdb-tree'}
META_TABLE=${META_TABLE-'tsdb-meta'}


hbh=$HBASE_HOME
unset HBASE_HOME
exec "$hbh/bin/hbase" shell <<EOF

disable '$UID_TABLE'

disable '$TSDB_TABLE'
  
disable '$TREE_TABLE'
  
disable '$META_TABLE'


drop '$UID_TABLE'

drop '$TSDB_TABLE'
  
drop '$TREE_TABLE'
  
drop '$META_TABLE'

EOF