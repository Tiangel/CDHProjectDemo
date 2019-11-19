package com.cloudera.vms.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;


import com.cloudera.vms.Config;


public class CheckpointUtil {
	
	
	public static boolean persist(String key,Object obj){
		String path = Config.get(Config.KEY_JOB_CHECKPOINT_URL)+"/"+key;
		  File file = new File(path);  
		  FileOutputStream fos = null;
		  ObjectOutputStream oos = null;
		 try  
		  {  
		   System.out.println("check point to"+path+" begin##");
		   
		   if(!file.exists()){
			   file.createNewFile(); 
		   }
		
		   fos = new FileOutputStream(file);  
		   oos = new ObjectOutputStream(fos);  
		   oos.writeObject(obj);  
		   oos.flush();  
		   oos.close();  
		   fos.close(); 
		   System.out.println("check point to"+path+" end##");
		   return true;
		  }  
		  catch(Throwable e)  
		  {  
		   e.printStackTrace();
		   return false;
		  }finally{
			  if(null!=oos){
				  try {
					oos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  }
			  if(null!=fos){
				  try {
					fos.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  }
		  }
	}
	
	
	public static Object get(String key){
		String path = Config.get(Config.KEY_JOB_CHECKPOINT_URL)+"/"+key;
		 File file = new File(path); 
		 if(!file.exists()){
			 return null;
		 }
		FileInputStream fis = null;
		 ObjectInputStream ois = null;
		try{
		       fis = new FileInputStream(file);  
			   ois = new ObjectInputStream(fis);  
			   return ois.readObject();  
			
		}catch(Throwable e){
			e.printStackTrace();
			return null;
		}finally{
			if(null!=fis){				
				   try {
					fis.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} 
			}
			if(null!=ois){
				 try {
					ois.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}  
			}
		}
		
		
		  
	}
	
	public static void main(String[] args){
		get("topics");
		
	}

}
