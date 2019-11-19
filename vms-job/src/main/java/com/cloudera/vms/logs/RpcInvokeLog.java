package com.cloudera.vms.logs;


@LogBean( placeholder = '-', separator = '	', size = 40)
public class RpcInvokeLog {
	@LogElement(rank = 0, description = "日志编号")
	String logCode = "log_sparkflow_data";// 日志编号
	@LogElement(rank = 1, description = "应用编号")
	String sysCode = "dmp-sparkflow-server";// 应用编号
	@LogElement(rank = 2, description = "业务分类")
	String bizType = "rpc-server";// 业务分类
	@LogElement(rank = 3, description = "日志级别")
	String logLevel ;// 日志级别
	@LogElement(rank = 4, description = "全链路key")
	String lnkKey;
	@LogElement(rank = 5, description = "日志记录时间")
	String logTime;
	@LogElement(rank = 6, description = "数据接收时间")
	String beginTime;
	@LogElement(rank = 7, description = "数据发送时间")
	String endTime;
	@LogElement(rank = 8, description = "处理流程编号")
	String processNum;
	@LogElement(rank = 9, description = "服务端ip")
	String serverIp;
	@LogElement(rank = 10, description = "日志内容")
	String logMsg;
	@LogElement(rank = 11, description = "耗时")
	Long useTime;
	@LogElement(rank = 12, description = "请求长度")
	Long requestLength;
	@LogElement(rank = 13, description = "相应长度")
	Long responseLength;
	@LogElement(rank = 14, description = "状态")
	String logStatus;
	@LogElement(rank = 15, description = "请求地址")
	String requestUrl;
	
	@LogElement(rank = 39, description = "额外信息,json格式")
	String extMsg;


	public String getLogLevel() {
		return logLevel;
	}

	public void setLogLevel(String logLevel) {
		this.logLevel = logLevel;
	}

	public String getLnkKey() {
		return lnkKey;
	}

	public void setLnkKey(String lnkKey) {
		this.lnkKey = lnkKey;
	}

	public String getLogTime() {
		return logTime;
	}

	public void setLogTime(String logTime) {
		this.logTime = logTime;
	}

	public String getBeginTime() {
		return beginTime;
	}

	public void setBeginTime(String beginTime) {
		this.beginTime = beginTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getProcessNum() {
		return processNum;
	}

	public void setProcessNum(String processNum) {
		this.processNum = processNum;
	}

	public String getServerIp() {
		return serverIp;
	}

	public void setServerIp(String serverIp) {
		/*try {
			this.serverIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}*/
		this.serverIp =serverIp;
	}

	public String getLogMsg() {
		return logMsg;
	}

	public void setLogMsg(String logMsg) {
		this.logMsg = logMsg;
	}

	public Long getUseTime() {
		return useTime;
	}

	public void setUseTime(Long useTime) {
		this.useTime = useTime;
	}

	public Long getRequestLength() {
		return requestLength;
	}

	public void setRequestLength(Long requestLength) {
		this.requestLength = requestLength;
	}

	public Long getResponseLength() {
		return responseLength;
	}

	public void setResponseLength(Long responseLength) {
		this.responseLength = responseLength;
	}

	public String getLogStatus() {
		return logStatus;
	}

	public void setLogStatus(String logStatus) {
		this.logStatus = logStatus;
	}

	public String getRequestUrl() {
		return requestUrl;
	}

	public void setRequestUrl(String requestUrl) {
		this.requestUrl = requestUrl;
	}

	public String getExtMsg() {
		return extMsg;
	}

	public void setExtMsg(String extMsg) {
		this.extMsg = extMsg;
	}
	
}
