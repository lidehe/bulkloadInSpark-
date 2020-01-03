package com.netstars.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * @author lidehe
 * Oct 30,2019
 */
public class PropertyUtils {
    static Logger log= LogManager.getLogger(PropertyUtils.class);
    static Properties properties = new Properties();
    static private String tableName;
    static {
        try {
            properties.load(PropertyUtils.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置要导入 HBase 的表，给mapreduce类读取，因为无法直接传递过去
     * @return
     */
    public static void setTableName(String curren_tableName){
        tableName=curren_tableName;
    }



    public static String getZKQuorum(){
        return properties.getProperty(tableName+Constants.zkQuorum);
    }

    public static String getZKPort(){
        return properties.getProperty(tableName+Constants.zkPort);
    }


    /**
     * 获取列族
     * @return
     */
    public static String getColumnFamily() {
        return properties.getProperty(tableName + Constants.columnFamilySuffix);
    }

    /**
     * 获取修饰词
     * @return
     */
    public static String getQualifiers() {
        return properties.getProperty(tableName  + Constants.qualifiersSuffix);
    }

    /**
     * 获取从DB2导出文件的列分隔符
     * @return
     */
    public static String getDelimiter() {
        return properties.getProperty(tableName +Constants.delimiterSuffix);
    }

    /**
     * 获取待转换成HBaseFile的目标文件，该文件由shell脚本导出后，通过hadoop命令上传到hdfs
     * 放在tmp下比较好
     * @return
     */
    public static String getInputFile() {
        return properties.getProperty(tableName  + Constants.inputFileSuffix);
    }

    /**
     * 获取转换后结果文件的保存位置，最好不要放在tmp下，容易被清除
     * @return
     */
    public static String getOutputFile() {
        return properties.getProperty(tableName  + Constants.outputFileSuffix);
    }
}
