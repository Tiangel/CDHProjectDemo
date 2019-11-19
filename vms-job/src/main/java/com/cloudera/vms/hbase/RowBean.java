package com.cloudera.vms.hbase;

public class RowBean {
	
	private String row;
	
	private Long timestamp;
 
	private String columnFamily;
	
	private String column;
	
	private String value;

	public String getRow() {
		return row;
	}

	public void setRow(String row) {
		this.row = row;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public void setColumnFamily(String columnFamily) {
		this.columnFamily = columnFamily;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
}
