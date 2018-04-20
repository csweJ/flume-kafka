package com.adtec.flume.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParseFileUtil {
	
	private static Logger logger=LoggerFactory.getLogger(ParseFileUtil.class);
	/**
	 * 解析文件并生成新的文件
	 * @param map
	 * @throws Exception
	 */
	public static void parseAndCreateFile(Map<String, Object> map,String flumeDirPath) throws Exception{
		//加载配置文件模板
		InputStream ins=ParseFileUtil.class.getClassLoader().getResourceAsStream("flume.conf.template");
		Properties prop=new Properties();
		prop.load(ins);
		
		String propKeyStr;
		
		String value;
		//生成新的配置文件的properties
		Properties newProp=new Properties();
		for(Object obj: prop.keySet()){
			//模板properties 的key
			propKeyStr=obj.toString();
			//模板properties value
			value=(String) prop.get(obj);
			//判断value 中是否有值需要替换
			if(value.contains("$")){
				String mapKey=value.substring(value.indexOf("{")+1, value.indexOf("}")).trim();
				//得到传进来的值
				value=(String) map.get(mapKey);
			}
			
			if(value==null||value.isEmpty()){
				continue;
			}
			
			//设置到新的配置文件的properties
			newProp.setProperty(getPropKey(map,propKeyStr), value);
			
		}
		
		//创建文件
		createPropFile(map, newProp,flumeDirPath);
	}
	
	/**
	 * 根据传的参数生成新的key
	 * @param map
	 * @param propKeyStr
	 * @return
	 */
	public static String getPropKey(Map<String, Object> map,String propKeyStr){
		//根据'.'拆分 propKeyStr
		String [] propChildKeyStrs=propKeyStr.split("\\.");
		StringBuffer sb=new StringBuffer();
		for(int i=0;i<propChildKeyStrs.length;i++){
			
			String propChildKeyStr=propChildKeyStrs[i];
			//替换值
			if(propChildKeyStr.contains("$")){
				String mapKey=propChildKeyStr.substring(propChildKeyStr.indexOf("{")+1, propChildKeyStr.indexOf("}")).trim();
				propChildKeyStr=propChildKeyStr.replace("${"+mapKey+"}", (String)map.get(mapKey));
			}
			//拼接key
			if(propChildKeyStrs.length==i+1){
				sb.append(propChildKeyStr);
			}else{
				sb.append(propChildKeyStr+".");
			}
		}
		return sb.toString();
		
	}
	/**
	 * 根据prop创建配置文件 存放在flume中conf下
	 * @param map
	 * @param prop
	 * @throws Exception
	 */
	public static void createPropFile(Map<String, Object> map ,Properties prop,String flumeDirPath) throws Exception {
		String fileName="flume_"+(String) map.get("sourceTypeName")+".conf";
		
		File file=new File(flumeDirPath+"conf\\"+fileName);
		//如果存在，删除
		if(file.exists()) {
		    file.delete();
		}
		
		FileOutputStream fos=new FileOutputStream(file);
		prop.store(fos, "根据模板生成flume配置文件");  
		logger.info("配置文件已经生成");
	}
	
	/**
	 * 
	 * @Title: compressed
	 * @Description: 压缩文件 或者文件夹
	 * @param @param srcFile
	 * @param @param tarOut
	 * @param @param parentPath
	 * @return void
	 * @throws
	 */
	public static void compressed(File srcFile,TarArchiveOutputStream tarOut,String parentPath) {
	    //判断是目录还是文件
	    if(srcFile.isDirectory()) {
	        tarDirectory(srcFile,tarOut,parentPath);
	    }else {
	        tarFile(srcFile, tarOut, parentPath);  
	    }
	}
	
	/**
	 * 
	 * @Title: tarDirectory
	 * @Description: 递归压缩文件夹
	 * @param @param sourceDir
	 * @param @param tout
	 * @param @param parentPath
	 * @return void
	 * @throws
	 */
	public static void tarDirectory(File sourceDir,TarArchiveOutputStream tout,String parentPath) {
	    if(!sourceDir.exists()) {
	        return;
	    }
	    if(sourceDir.listFiles().length < 1){  
            TarArchiveEntry entry = new TarArchiveEntry(parentPath + sourceDir.getName() + "\\");  
            try {  
                tout.putArchiveEntry(entry);  
                tout.closeArchiveEntry();  
            } catch (IOException e) {  
                logger.error(e.getMessage(),e);  
            }  
        }  
        //递归 归档   
        for (File file : sourceDir.listFiles()) {  
            compressed(file, tout,parentPath + sourceDir.getName() + "\\");  
        }  
	}
	
	/**
	 * 
	 * @Title: tarFile
	 * @Description: 压缩文件
	 * @param @param file
	 * @param @param tout
	 * @param @param parentPath
	 * @return void
	 * @throws
	 */
	public static void tarFile(File file,TarArchiveOutputStream tout,String parentPath) {
	    if(file.isDirectory()) {
	        return;
	    }
	    byte[] buff=new byte[1024];
	    
        FileInputStream fis=null;
        //打开需压缩文件作为文件输入流  
        try {
            fis=new FileInputStream(file);
            int num;
            TarArchiveEntry tarArchiveEntry=new TarArchiveEntry(parentPath+file.getName());
            tarArchiveEntry.setSize(file.length());
            tout.putArchiveEntry(tarArchiveEntry);
           
            while((num=fis.read(buff))!=-1) {
                tout.write(buff,0,num);
            }
            tout.closeArchiveEntry();  
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }finally {
            try {
                if(fis!=null) {
                    fis.close();
                }
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }
	}
	
	/**
	 * 
	 * @Title: CompressedFilesWithGzip
	 * @Description: 压缩生成gzip文件，先生成tar文件
	 * @param @param srcFolderPath
	 * @param @param tarFile
	 * @param @throws Exception
	 * @return void
	 * @throws
	 */
	public static void compressedFilesWithGzip(String srcFolderPath,String tarFile,String parentPath) {
	    File srcFile=new File(srcFolderPath);
	    
	    byte[] buff=new byte[1024];
	    FileOutputStream fos=null;
	    TarArchiveOutputStream tarArchiveOutputStream=null;
	    //建立tar压缩文件输出流
	    try {
            fos=new FileOutputStream(tarFile);
            tarArchiveOutputStream=new TarArchiveOutputStream(fos);
            tarArchiveOutputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU); 
            //打开需压缩文件 tar文件作为文件输入流 
            compressed(srcFile, tarArchiveOutputStream, parentPath);
            tarArchiveOutputStream.flush();
            tarArchiveOutputStream.close();
        } catch (FileNotFoundException e1) {
            logger.error(e1.getMessage());
        } catch (IOException e1) {
            logger.error(e1.getMessage());
        }finally {
            try {
                if(fos!=null) {
                    fos.close();
                }
                if(tarArchiveOutputStream!=null) {
                    tarArchiveOutputStream.close();
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    
	    //建立gzip压缩文件输出流
	    FileOutputStream gzFos=null;
	    GZIPOutputStream gzout=null;
	    
	    FileInputStream gzFin=null;
	    
	    try {
            gzFos=new FileOutputStream(tarFile+".gz");
            gzout=new GZIPOutputStream(gzFos);
            
            gzFin=new FileInputStream(tarFile);
            
            int len;
            while((len=gzFin.read(buff))!=-1) {
                gzout.write(buff, 0, len);
            }
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage());
        } catch (IOException e) {
            logger.error(e.getMessage());
        }finally {
            try {
                gzout.close();
                gzFos.close();
                gzFin.close();
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
	    
	}
	
	
	public static void main(String[] args) throws Exception {
//		parseFile();
		Map<String, Object> map=new HashMap<String, Object>();
		
		map.put("agentName", "agent1");
		map.put("sourceName", "src");
		map.put("channelName", "ch");
		map.put("sinkName", "sink");
		
		//spoolDir source
//		map.put("sourceTypeName", "spoolDir");
//		map.put("sourceTypeClass", "spoolDir");
//		map.put("spoolDirName", "/opt/apache-flume-1.8.0-bin/test");
//		map.put("fileHeader", "true");
//		map.put("fileHeaderKey", "fileName");
//		map.put("batchSize", "10");
//		map.put("ignorePattern", "^$");
//		map.put("isRecursiveDirectorySearch", "true");
		
		map.put("sourceTypeName", "sql");//自定义
        map.put("sourceTypeClass", "org.keedio.flume.source.SQLSource");
        map.put("url", "jdbc:mysql://10.10.20.105:3306/test");
        map.put("driverClass", "com.mysql.jdbc.Driver");
        //dialect配置见：https://docs.jboss.org/hibernate/orm/4.3/manual/en-US/html/ch03.html#configuration-optional-dialects
        map.put("dialect", "org.hibernate.dialect.MySQL5Dialect");
        map.put("user", "hds");
        map.put("password", "123456");
        map.put("table", "customerinfo");
        map.put("selectColumns", "*");
        map.put("incrementalCol", "messageNO");
        map.put("incrementalVal", "0");
        map.put("delay", "5000");//每隔多少秒抽取一次
        map.put("statusFilePath", "/opt/ficlientC70/tmp/flume");
        map.put("statusFileName", "sql-source.status");
        map.put("maxRows", "1000");
        map.put("delimiter", "|");//分隔符
        map.put("hasQuotes", "false");//分隔符
       
		//taildir source
//		map.put("sourceTypeName", "tailDir");
//		map.put("sourceTypeClass", "TAILDIR");
//		map.put("fileHeader", "true");
//		map.put("fileHeaderKey", "filename");
//		map.put("batchSize", "200");
//		map.put("positionFile", "/opt/apache-flume-1.8.0-bin/tmp/position/testlog/taildir_position.json");
//		map.put("filegroup", "f1");
//		map.put("file", "/opt/apache-flume-1.8.0-bin/test/.*");
		
		
		//syslog source
//		map.put("sourceTypeName", "syslogtcp");
//		map.put("sourceTypeClass", "syslogtcp");
//		map.put("syslogHost", "192.168.31.221");
//		map.put("syslogPorts", "5000");
//		map.put("eventSize", "200");
		
		
		//channels
		map.put("capacity", "100000");
		map.put("transactionCapacity", "100000");
		//sinks
		map.put("brokerList", "10.10.20.101:21005,10.10.20.102:21005,10.10.20.103:21005,10.10.20.104:21005");
		map.put("topicName", "new_test_topic");
		
		
		String workPath=System.getProperty("user.dir");
		String flumeDirPath=workPath+"\\apache-flume-1.8.0-bin\\";
		String tarPackage=workPath+"\\flume.tar";
		
		System.out.println(flumeDirPath);
		parseAndCreateFile(map,flumeDirPath);
		compressedFilesWithGzip(flumeDirPath,tarPackage,"");
	}
}
