package com.cloudera.vms.hbase;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.logging.log4j.LogManager;


import org.apache.logging.log4j.Logger;
import org.junit.Test;


public class HBaseJavaAPI {

	private static final Logger logger =  LogManager.getLogger(HBaseJavaAPI.class);
	
	/** 所有表都拥有的列族 */
	public static final String columnFamily = "info";
	
	/** 行主键 */
	public static final String row = "row";

	// 声明静态配置
	private static Configuration conf = null;

	

	static {
		conf = HBaseConfiguration.create();
//		conf.set("hbase.zookeeper.quorum","10.248.161.21,10.248.161.22,10.248.161.23");
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
//		logger.info("init hbase config zk={},port={}",
//				conf.get("hbase.zookeeper.quorum"),
//				conf.get("hbase.zookeeper.property.clientPort"));
		
		
	}
	
	
	
	public static Configuration getConf() {
		return conf;
	}


	public Connection getHbaseConnection(){
		Connection connection = null;
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			logger.error("failed to get hbase connection", e);
		}
		return connection;
	}
	
	public void closeHbaseConnection(Connection connection) throws IOException{
		if(null!=connection){
			connection.close();
		}	
	}
	
	public Table getHTable(Connection connection,String tableName) throws IOException{
		Table table = connection.getTable(TableName.valueOf(tableName));
		return table;
		
	}
	
	public  void addRow(Table table, String row,
			String columnFamily, Map<String, String> columns) throws IOException {
		
		try {
		
			Put put = new Put(Bytes.toBytes(row));// 指定行
			// 参数分别:列族、列、值
			for(String key : columns.keySet() ) {
				put.addColumn(columnFamily.getBytes(), key.getBytes(), columns.get(key).getBytes());
				
			}
			table.put(put);
		} catch (IOException e) {
			throw e;
		
		} finally {
			
		}
	}
	
	public Map<String,String> getRow(Table table,String rowkey) throws IOException{
		Get get = new Get(Bytes.toBytes(rowkey));
		Result r = table.get(get);
		Map<String,String> map = new HashMap<String,String>();
	
		for (Cell cell : r.rawCells()) {
				map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
		}
		return map;
	}
	
	
	public List<Map<String,String>> getRowsByFuzzyRowkey(String tableName,String rowKey){
		
		List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
		HTable table = null;
		try {
		    table = new HTable(conf, tableName);
			Scan scan = new Scan();
			RowFilter filter = new RowFilter(CompareOp.EQUAL,new RegexStringComparator(rowKey));
			scan.setFilter(filter);
			
			ResultScanner results = table.getScanner(scan);
			// 输出结果
			for (Result result : results) {
				Map<String, String> map = new HashMap<String, String>();
				for (Cell cell : result.rawCells()) {
					map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
				}
				map.put(row, new String(result.getRow()));
				lists.add(map);
			}
		} catch (IOException e) {
			logger.error("getRowsByCondition has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return lists;
		
	}
	
	public  List<Map<String, String>> getRowsByConditions(Table table, String columnFamily,
			Map<String, String> conditions) {
		
		List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
		try {
		
			Scan scan = new Scan();
			FilterList filterList = new FilterList();
			for (String key : conditions.keySet()) {
				Filter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
						Bytes.toBytes(key), CompareOp.EQUAL, Bytes.toBytes(conditions
								.get(key)));
				filterList.addFilter(filter);
			}
			scan.setFilter(filterList);
			
			ResultScanner results = table.getScanner(scan);
			// 输出结果
			for (Result result : results) {
				Map<String, String> map = new HashMap<String, String>();
				for (Cell cell : result.rawCells()) {
					map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
				}
				map.put(row, new String(result.getRow()));
				lists.add(map);
			}
		} catch (IOException e) {
			logger.error("getRowsByCondition has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return lists;
	}
	
	
	

	public String getAttr(Table table,String rowkey,String cf,String attr) throws IOException{
		Get get = new Get(Bytes.toBytes(rowkey));
		Result r = table.get(get);
	
		byte[] b = r.getValue(Bytes.toBytes(cf), Bytes.toBytes(attr));
		return new String(b);
	}
	// 判断表是否存在
	public static boolean isExist(String tableName) {
		HBaseAdmin hAdmin = null;
		boolean isExist = false;
		try {
			hAdmin = new HBaseAdmin(conf);
			isExist = hAdmin.tableExists(tableName);
		} catch (IOException e) {
			logger.error("determine table is exist has exception={}", e);
		} finally {
			if (hAdmin != null) {
				try {
					hAdmin.close();
					hAdmin = null;
				} catch (IOException e) {
					logger.error("close HBaseAdmin has exception={}", e);
				}
			}
		}
		return isExist;
	}

	// 创建数据库表
	public static void createTable(String tableName, String[] columnFamilys) {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = null;
		try {
			hAdmin = new HBaseAdmin(conf);
			if (hAdmin.tableExists(tableName)) {
				logger.info("table={} has exists.", tableName);
				return;
			} else {
				// 新建一个表的描述
				HTableDescriptor tableDesc = new HTableDescriptor(
						TableName.valueOf(tableName));
				// 在描述里添加列族
				for (String columnFamily : columnFamilys) {
					tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				}
				// 根据配置好的描述建表
				hAdmin.createTable(tableDesc);
				logger.info("create table={} success.", tableName);
			}
		} catch (IOException e) {
			logger.error("createTable table has exception={}", e);
		} finally {
			if (hAdmin != null) {
				try {
					hAdmin.close();
				} catch (IOException e) {
					logger.error("close HBaseAdmin has exception={}", e);
				}
			}
		}
	}

	// 删除数据库表
	public static void deleteTable(String tableName) {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = null;
		try {
			hAdmin = new HBaseAdmin(conf);
			if (hAdmin.tableExists(tableName)) {
				// 关闭一个表
				hAdmin.disableTable(tableName);
				hAdmin.deleteTable(tableName);
				logger.info("delete table={} success.", tableName);
			} else {
				logger.info("this table={} not exist.", tableName);
				return;
			}
		} catch (IOException e) {
			logger.error("deleteTable table has exception={}", e);
		} finally {
			if (hAdmin != null) {
				try {
					hAdmin.close();
					hAdmin = null;
				} catch (IOException e) {
					logger.error("close HBaseAdmin has exception={}", e);
				}
			}
		}
	}

	/**
	 * 添加一条数据
	 * @param tableName
	 * @param row
	 * @param columnFamily
	 * @param columns
	 */
	public static void addRow(String tableName, String row,
			String columnFamily, Map<String, String> columns) {
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(row));// 指定行
			// 参数分别:列族、列、值
			for(String key : columns.keySet() ) {
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(key),
						Bytes.toBytes(columns.get(key)));
			}
			table.put(put);
		} catch (IOException e) {
			logger.error("addRow has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
	}
	
	
	public static void addRows(String tableName,List<RowBean> rows){
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			table.setAutoFlush(false);
			int i=0;
			for(RowBean rowBean:rows){
				// 参数分别:列族、列、值
				Put put = new Put(Bytes.toBytes(rowBean.getRow()));// 指定行
			
			    put.add(Bytes.toBytes(rowBean.getColumnFamily()), Bytes.toBytes(rowBean.getColumn()),
							Bytes.toBytes(rowBean.getValue()));
				
				i++;	
			  table.put(put);
			  if(i%10000==0){
				  table.flushCommits();
				  logger.info("persist 10000 records success");
				  
				  
			  }
			}	
			 table.flushCommits();
			 logger.info("persist all records success");
		} catch (IOException e) {
			logger.error("addRow has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
	}

	/**
	 * 更新一条数据
	 * @param tableName 表明
	 * @param row 行
	 * @param columnFamily 列族
	 * @param columns 具体列值对
	 */
	public static void updateRow(String tableName, String row,
			String columnFamily, Map<String, String> columns) {
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(row));// 指定行
			// 参数分别:列族、列、值
			for(String key : columns.keySet()) {
				put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(key),
						Bytes.toBytes(columns.get(key)));
			}
			table.put(put);
		} catch (IOException e) {
			logger.error("addRow has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
	}

	/**
	 * 删除一条(行)数据
	 * @param tableName
	 * @param row
	 */
	public static void delRow(String tableName, String row) {
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			Delete del = new Delete(Bytes.toBytes(row));
			table.delete(del);
		} catch (IOException e) {
			logger.error("delRow has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
	}
	
	/**
	 * 根据条件进行删除
	 * @param tableName
	 * @param columnFamily
	 * @param conditions
	 */
	public static void delRowByCondition(String tableName, String columnFamily, Map<String, String> conditions) {
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			List<Map<String, String>> results = getRowsByCondition(tableName, columnFamily, conditions);
			List<Delete> deletes = new ArrayList<Delete>();
			for(Map<String, String> map : results) {
				String id = map.get(row);
				if(id != null && !"".equals(id)) {
					Delete del = new Delete(Bytes.toBytes(id));
					deletes.add(del);
				}
			}
			table.delete(deletes);
		} catch (IOException e) {
			logger.error("delRow has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
	}

	// 删除多条数据
	public static void delMultiRows(String tableName, String[] rows) {
		HTable table = null;
		try {
			table = new HTable(conf, tableName);
			List<Delete> delList = new ArrayList<Delete>();
			for (String row : rows) {
				Delete del = new Delete(Bytes.toBytes(row));
				delList.add(del);
			}
			table.delete(delList);
		} catch (IOException e) {
			logger.error("delMultiRows has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
	}

	/**
	 * 获取一条数据的详情
	 * @param tableName
	 * @param row
	 * @return
	 */
	public static List<RowBean> getRowDetail(String tableName, String row) {
		HTable table = null;
		List<RowBean> rows = new ArrayList<RowBean>();
		try {
			table = new HTable(conf, tableName);
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);
			// 输出结果,raw方法返回所有keyvalue数组
			for (Cell cell : result.rawCells()) {
				RowBean rowbean = new RowBean();
				rowbean.setRow(new String(result.getRow()));
				rowbean.setTimestamp(cell.getTimestamp());
				rowbean.setColumnFamily(new String(CellUtil.cloneFamily(cell)));
				rowbean.setColumn(new String(CellUtil.cloneQualifier(cell)));
				rowbean.setValue(new String(CellUtil.cloneValue(cell)));
				rows.add(rowbean);
			}
		} catch (IOException e) {
			logger.error("getRowDetail has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return rows;
	}
	
	/**
	 * 获取一条记录（所有列）
	 * @param tableName
	 * @param row
	 * @return
	 */
	public static Map<String, String> getRow(String tableName, String row) {
		HTable table = null;
		Map<String, String> map = new HashMap<String, String>();
		try {
			table = new HTable(conf, tableName);
			Get get = new Get(Bytes.toBytes(row));
			Result result = table.get(get);
			// 输出结果,raw方法返回所有keyvalue数组
			for (Cell cell : result.rawCells()) {
				map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
			}
		} catch (IOException e) {
			logger.error("getRow has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return map;
	}

	/**
	 * 获取所有数据详情
	 * @param tableName
	 * @return
	 */
	public static List<List<RowBean>> getAllRowsDetail(String tableName) {
		HTable table = null;
		List<List<RowBean>> lists = new ArrayList<List<RowBean>>();
		try {
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner results = table.getScanner(scan);
			// 输出结果
			for (Result result : results) {
				List<RowBean> rows = new ArrayList<RowBean>();
				for (Cell cell : result.rawCells()) {
					RowBean rowbean = new RowBean();
					rowbean.setRow(new String(result.getRow()));
					rowbean.setTimestamp(cell.getTimestamp());
					rowbean.setColumnFamily(new String(CellUtil
							.cloneFamily(cell)));
					rowbean.setColumn(new String(CellUtil.cloneQualifier(cell)));
					rowbean.setValue(new String(CellUtil.cloneValue(cell)));
					rows.add(rowbean);
				}
				lists.add(rows);
			}
		} catch (IOException e) {
			logger.error("getAllRowsDetail has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return lists;
	}
	
	/**
	 * 获取所有行数据
	 * @param tableName
	 * @return
	 */
	public static List<Map<String, String>> getAllRows(String tableName) {
		HTable table = null;
		List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
		try {
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			ResultScanner results = table.getScanner(scan);
			// 输出结果
			for (Result result : results) {
				Map<String, String> map = new HashMap<String, String>();
				for (Cell cell : result.rawCells()) {
					map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
				}
				map.put(row, new String(result.getRow()));
				lists.add(map);
			}
		} catch (IOException e) {
			logger.error("getAllRows has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return lists;
	}

	/**
	 * 根据条件获取数据
	 * @param tableName
	 * @param columnFamily
	 * @param conditions
	 * @return
	 */
	public static List<List<RowBean>> getRowsDetailByCondition(String tableName, String columnFamily,
			Map<String, String> conditions) {
		HTable table = null;
		List<List<RowBean>> lists = new ArrayList<List<RowBean>>();
		try {
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			FilterList filterList = new FilterList();
			for (String key : conditions.keySet()) {
				Filter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
						Bytes.toBytes(key), CompareOp.EQUAL, Bytes.toBytes(conditions
								.get(key)));
				filterList.addFilter(filter);
			}
			scan.setFilter(filterList);
			ResultScanner results = table.getScanner(scan);
			// 输出结果
			for (Result result : results) {
				List<RowBean> rows = new ArrayList<RowBean>();
				for (Cell cell : result.rawCells()) {
					RowBean rowbean = new RowBean();
					rowbean.setRow(new String(result.getRow()));
					rowbean.setTimestamp(cell.getTimestamp());
					rowbean.setColumnFamily(new String(CellUtil
							.cloneFamily(cell)));
					rowbean.setColumn(new String(CellUtil.cloneQualifier(cell)));
					rowbean.setValue(new String(CellUtil.cloneValue(cell)));
					rows.add(rowbean);
				}
				lists.add(rows);
			}
		} catch (IOException e) {
			logger.error("getRowsDetailByCondition has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return lists;
	}
	
	/**
	 * 根据条件获取数据
	 * @param tableName
	 * @param columnFamily
	 * @param conditions
	 * @return
	 */
	public static List<Map<String, String>> getRowsByCondition(String tableName, String columnFamily,
			Map<String, String> conditions) {
		HTable table = null;
		List<Map<String, String>> lists = new ArrayList<Map<String, String>>();
		try {
			table = new HTable(conf, tableName);
			Scan scan = new Scan();
			FilterList filterList = new FilterList();
			for (String key : conditions.keySet()) {
				Filter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
						Bytes.toBytes(key), CompareOp.EQUAL, Bytes.toBytes(conditions
								.get(key)));
				filterList.addFilter(filter);
			}
			//scan.setTimeRange(1471943130538l, 1471943130540l);
			//scan.setFilter(filterList);
			scan.setFilter(new  PageFilter(1));

			scan.setReversed(true);
			 scan.setMaxVersions(1);
			ResultScanner results = table.getScanner(scan);
			// 输出结果
			for (Result result : results) {
				Map<String, String> map = new HashMap<String, String>();
				for (Cell cell : result.rawCells()) {
					map.put(new String(CellUtil.cloneQualifier(cell)), new String(CellUtil.cloneValue(cell)));
				}
				map.put(row, new String(result.getRow()));
				lists.add(map);
			}
		} catch (IOException e) {
			logger.error("getRowsByCondition has exception={}", e);
		} finally {
			if (table != null) {
				try {
					table.close();
					table = null;
				} catch (IOException e) {
					logger.error("close HTable has exception={}", e);
				}
			}
		}
		return lists;
	}
	
	
	
	public static void main(String[] args) {
		HBaseJavaAPI.createTable("ams_emotion_change",new String[] {"fn"});
//		Map<String,String> columns = new HashMap<String,String>();
//		columns.put("hanmingCode", "9");
//		logger.info("start add..");
//		HBaseJavaAPI.addRow("spark2", "0", "hanmingCode", columns);
//		logger.info("end add..");
//		List<RowBean> rowBeans = new ArrayList<RowBean>();
//		for(int i=0;i<100000;i++){
//			RowBean row = new RowBean();
//			row.setColumn("code");
//			row.setRow(i+"");
//			row.setColumnFamily("hanmingCode");
//			row.setValue(i+"");
//			rowBeans.add(row);
//		}
//		HBaseJavaAPI.addRows("test2", rowBeans);
		
		List<RowBean> rowBeans = new ArrayList<RowBean>();
		
		RowBean subIdRow = new RowBean();
		subIdRow.setColumn("emotion");
		subIdRow.setRow("110_10");
		subIdRow.setColumnFamily("fn");
		subIdRow.setValue("1");
		
//		RowBean midRow = new RowBean();
//		midRow.setColumn("mid");
//		midRow.setRow("2_0_3705896540761925");
//		midRow.setColumnFamily("fn");
//		midRow.setValue("0_3705896540761925");
//		
		rowBeans.add(subIdRow);
	//	rowBeans.add(midRow);
		
//		RowBean topicRow = new RowBean();
//		topicRow.setColumn("mid");
//		topicRow.setRow("0_10689152554");
//		topicRow.setColumnFamily("fn");
//		topicRow.setValue("0_3355358798902818");
//		
		//HBaseJavaAPI.addRows("t_subject", rowBeans);
		
		
//		RowBean user = new RowBean();
//		user.setColumn("uid");
//		user.setRow("1");
//		user.setColumnFamily("info");
//		user.setValue("0_1646944977");
//		
	//	rowBeans.add(topicRow);
		
		
		HBaseJavaAPI.addRows("ams_emotion_change", rowBeans);
		
		
		Map<String,String> map = new HashMap<String,String>();
		//map.put("sub_id", "1");
		List<Map<String,String>> result = HBaseJavaAPI.getRowsByCondition("t_user", "fn", map);
		System.out.println(result.size());
		for(Map<String,String> data:result){
			for(String key:data.keySet()){
				System.out.println(key+":"+data.get(key));
			}
			System.out.println("---------------------");
		}
		
		
	}


	@Test
	public void t1(){
		Map<String,String> obj=HBaseJavaAPI.getRow("sina_weibo_user", "61de_1854240030" );
		System.out.println("obj:"+obj);
	}
}
