package net.opentsdb.core;

import org.hbase.async.AppendRequest;
import org.hbase.async.HBaseRpc;
import org.hbase.async.PutRequest;

import net.opentsdb.utils.Config;

public class RequestBuilder {
	
	public static PutRequest buildPutRequest(Config config, byte[] tableName, byte[] row, byte[] family, byte[] qualifier, byte[] value, long timestamp) {
		
		if(config.use_otsdb_timestamp()) 
			if((timestamp & Const.SECOND_MASK) != 0)
				return new PutRequest(tableName, row, family, qualifier, value, timestamp);
			else
				return new PutRequest(tableName, row, family, qualifier, value, timestamp * 1000);
		else
			return new PutRequest(tableName, row, family, qualifier, value);
	}

	public static AppendRequest buildAppendRequest(Config config, byte[] tableName, byte[] row, byte[] family, byte[] qualifier, byte[] value, long timestamp) {
		
		if(config.use_otsdb_timestamp())
			if((timestamp & Const.SECOND_MASK) != 0)
				return new AppendRequest(tableName, row, family, qualifier, value, timestamp);
			else
				return new AppendRequest(tableName, row, family, qualifier, value, timestamp * 1000);
		else
			return new AppendRequest(tableName, row, family, qualifier, value);
	}

}
