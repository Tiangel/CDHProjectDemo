package com.cloudera.vms.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;


public class FileUtils {


	
	public static void write2File(String path,String content,String encode,boolean append) throws IOException{
		String fileName = path.substring(path.lastIndexOf(File.separator)+1);
		
		String filePath = path.substring(0,path.lastIndexOf(File.separator));
		String tempFileName = "_"+fileName;
		File file = new File(append?path:(filePath+File.separator+tempFileName));
		file.getParentFile().mkdirs();
		if(!file.exists()){
			file.createNewFile();
		}
		 BufferedWriter bw = null;
		 try{
			 bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file,append),encode));
			 bw.append(content).append("\n");
		 }catch(Exception e){
			 e.printStackTrace();
		 }finally{
			 if(null!=bw){
				 bw.close();
			 }
			
		 }
		 if(file.exists()&&!append){
			 File destFile = new File(path);
			 if(destFile.exists()){
				 try{
					 destFile.delete();
				 }catch(Throwable e){
					 try {
							Thread.sleep(10000);
						} catch (InterruptedException e1) {
							e.printStackTrace();
						}
						 try{
							 destFile.delete();
						 }catch(Throwable e1){
							e.printStackTrace();
						 }
				 }
				 
				
			 }
			 file.renameTo(new File(path));
			 if(file.exists()){
				 file.delete();
			 }
		 }
	
		 
	}
	
	 public static List<String> readSource(String path) throws IOException{
		 return readSource(path,true);
	 }
	
    public static List<String> readSource(String path,boolean skipFirstLine) throws IOException{
		
		return readSource(path, skipFirstLine, "utf-8");
	}
    
    
   public static List<String> readSource(String path,boolean skipFirstLine,String encode) throws IOException{
		
		File file = new File(path);
		
		BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file),encode));
        String line = null;
     
        List<String> records = new ArrayList<String>();
        if(skipFirstLine){
        	  br.readLine();
        }
      
        while(null!=(line=br.readLine())){
        	records.add(line);
        }
        br.close();
		return records;
	}
	
   public static void main(String[] args) throws IOException {
	   FileUtils.write2File("d:\\peter\\a.txt", "abv", "utf-8", true);
   }

}
