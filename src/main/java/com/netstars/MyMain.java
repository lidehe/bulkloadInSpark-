package com.netstars;

import com.netstars.utils.PropertyUtils;

public class MyMain {


    public static void main(String[] args) {
        if (args.length != 2) {
            throw new RuntimeException("Parameters \"table name \" and \"data date(like 20120912)\" was expected");
        }
        String tableName = args[0];
        String date = args[1];
        String input;
        String output;
        String zkQuorum;//"vm156";//zookeeper集群
        String zkPort;//"2181";//zookeeper端口号
        String family;//列簇
        String delimiter;// 文件中 列 分隔符
        String qualifiers;// 列
        try {
            PropertyUtils.setTableName(tableName);
            input = PropertyUtils.getInputFile();
            output = PropertyUtils.getOutputFile();
            input = input.replaceAll("\\{date\\}", date);
            output = output.replaceAll("\\{date\\}", date);
            zkQuorum = PropertyUtils.getZKQuorum();//"vm156";//zookeeper集群
            zkPort = PropertyUtils.getZKPort();//"2181";//zookeeper端口号
            family = PropertyUtils.getColumnFamily();//列簇
            delimiter = PropertyUtils.getDelimiter();// 文件中 列 分隔符
            qualifiers = PropertyUtils.getQualifiers();// 列
        } catch (Exception e) {
            throw new RuntimeException("Parameters initial error,check configuration and input parameters please ! " + e);
        }

        // 调用转换
        /**
         *         String zkQuorum = params[0];//zookeeper集群
         *         String zkPort = params[1];//zookeeper端口号
         *         String inputPath = params[2];//输入路径
         *         String outputPath = params[3];//输入路径
         *         String tableName = params[4];//表名
         *         String family = params[5];//列簇
         *         String delimiter = params[6];// 文件中的列的分隔符
         *         String[] qualifiers = params[7].split(delimiter, -1);// 列
         */
        try {
            new SparkLoad().sparkLoad(zkQuorum, zkPort, input, output, tableName,family, delimiter, qualifiers.split(delimiter, -1));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
