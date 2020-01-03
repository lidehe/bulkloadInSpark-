package com.zxftech.rrms;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.tool.BulkLoadHFiles;
import org.apache.hadoop.hbase.tool.BulkLoadHFilesTool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class SparkLoad implements Serializable {

    /**
     * @param zkQuorum   zk集群IP， split by comma
     * @param zkPort     zk端口号
     * @param inputPath  待转换文件路径
     * @param outputPath 输出文件路径
     * @param tableName  HBase表名
     * @param family     列族
     * @param delimiter  列分割符
     * @param qualifiers 修饰符，列族:修饰符 定位到一个column， 映射关系  行键:列族:修饰符:version = cell
     * @throws Exception
     */
    public void sparkLoad(String zkQuorum, String zkPort, String inputPath, String outputPath, String tableName, String family, String delimiter, String[] qualifiers) throws Exception {
//        SparkSession spark = SparkSession.builder().master("spark://zx162:7077").appName("zzz").getOrCreate();
        SparkSession spark = SparkSession.builder().appName("RRMSRESULT-HBASE").getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> rdd = sc.textFile(inputPath);
        JavaPairRDD<ImmutableBytesWritable, KeyValue> flatMapToPair = rdd.mapToPair(record -> {
            return new Tuple2<>(record.split(delimiter)[0], record);
        }).reduceByKey((pre, next) -> {// 这两步去重，把要去重的字段map成key，再reduceByKey就好了
            return next;
        }).map(result -> {
            return result._2;
        }).sortBy(mresult -> { // 这个排序非常关键，如源数据主键未排序且这里也不排序，会报错 Added a key not lexically larger than previous
            return mresult.split(",")[0];
        }, true, 3).flatMapToPair(new PairFlatMapFunction<String, ImmutableBytesWritable, KeyValue>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Iterator<Tuple2<ImmutableBytesWritable, KeyValue>> call(String line) throws Exception {
                List<Tuple2<ImmutableBytesWritable, KeyValue>> kvs = new ArrayList<>();
                String[] cols = line.split(delimiter, -1);
                byte[] rowkey = (cols[0]).getBytes();//原表中的第一列的主键id当作行键
                // 要从第二列开始，因为第一列作为行键了
                for (int i = 1; i < qualifiers.length; i++) {
                    kvs.add(new Tuple2<>(new ImmutableBytesWritable(rowkey), new KeyValue(rowkey, family.getBytes(), qualifiers[i].getBytes(), cols[i].getBytes())));
                }
                return kvs.iterator();
            }
        });//.sortByKey();


        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkQuorum);
        conf.set("hbase.zookeeper.property.clientPort", zkPort);
        conf.set("hbase.defaults.for.version.skip", "true");
        conf.set(TableOutputFormat.OUTPUT_TABLE, tableName);
        conf.set("hbase.loadincremental.validate.hfile", "false");// 校验HFile,如果校验，会花费较长时间，但能确保数据正常，择机而定吧
        //        String hbaseHome = System.getenv("HBASE_HOME");
        //        String hbConf = hbaseHome + File.pathSeparator + "conf/hbase-site.xml";
        //        conf.addResource(new Path(hbConf));

        Job job = Job.getInstance(conf);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        FileSystem fs = FileSystem.newInstance(conf);
        if (fs.exists(new Path(outputPath))) {
            fs.delete(new Path(outputPath), true);
        }

        Connection connection = ConnectionFactory.createConnection(conf);
        HTable table = (HTable) connection.getTable(TableName.valueOf(tableName));
        HFileOutputFormat2.configureIncrementalLoad(job, table, table.getRegionLocator());
        HFileOutputFormat2.setOutputPath(job, new Path(outputPath));
        // 特别注意，要用job.getConfiguration()，而不是conf，因为上一步骤里改变了job里的conf，与原来的conf不一样了
        flatMapToPair.saveAsNewAPIHadoopDataset(job.getConfiguration());

        BulkLoadHFiles bulkLoadHFiles = new BulkLoadHFilesTool(job.getConfiguration());
        bulkLoadHFiles.bulkLoad(table.getName(), HFileOutputFormat2.getOutputPath(job));

        sc.close();
    }


}
