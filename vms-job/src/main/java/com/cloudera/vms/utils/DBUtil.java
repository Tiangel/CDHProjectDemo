package com.cloudera.vms.utils;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class DBUtil {
	
	public static Connection getConnection(){
		 String url = "jdbc:mysql://192.168.2.4:3306/weibo_dc" ;    
	     String username = "dev" ;   
	     String password = "dev" ;   
	     try{   
	    Connection con =    
	             DriverManager.getConnection(url , username , password ) ;   
	    return con;
	     }catch(SQLException se){   
	    System.out.println("数据库连接失败！");   
	    se.printStackTrace() ;   
	     }  
	     return null;
	}
	
	public static List<String> getWeiboList(int n) throws SQLException{
		Connection connection = getConnection();
		try{
			Statement stmt = connection.createStatement();
			ResultSet rs = stmt.executeQuery("select text from t_status_weibo_001 limit "+n);
	        List<String> weiboList = new ArrayList<String>();
			while(rs.next()){
				weiboList.add(rs.getString("text"));
			}
			return weiboList;
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(null!=connection){
				connection.close();
			}
		}
		return null;
		
	}
	
	public static void main(String[] args) throws SQLException {
		List<String> weibos = getWeiboList(10);
		for(String weibo:weibos){
			System.out.println(weibo);
		}
	}

}
