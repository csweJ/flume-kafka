package com.adtec.kafka;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;


public class Test {

    public static void main(String[] args) throws Exception {
        long startTime=System.currentTimeMillis();
        long num=1;
        long customerNo=10000L;
        while(num<=300000) {
            long timestamp=System.currentTimeMillis();
            String messageStr=customerNo+","+timestamp+","+num;
            
            //创建文件
            String filePath="G:\\kafkadata.txt";
            File file=new File(filePath);
            FileWriter out=new FileWriter(file,true);
            BufferedWriter bw=new BufferedWriter(out);
            
            bw.write(messageStr);
            bw.newLine();
          
            bw.flush();
            bw.close();
            customerNo++;
            if(customerNo>10099) {
                customerNo=10000L;
            }
            num++;
        }
        System.out.println(System.currentTimeMillis()-startTime);

    }

}
