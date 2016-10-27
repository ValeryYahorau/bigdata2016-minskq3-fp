package com.epam.bigdata2016fp.sparkstreaming.hbase;

import com.epam.bigdata2016fp.sparkstreaming.conf.AppProperties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseProcessor implements Serializable {

    public static void saveToTable(Iterator<Put> iter, AppProperties.Hbase hbaseConfig) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", hbaseConfig.getZookeeperClientPort());
        conf.set("hbase.zookeeper.quorum", hbaseConfig.getZookeeperQuorum());
        conf.set("zookeeper.znode.parent", hbaseConfig.getZookeeperParent());

        try (Connection connection = ConnectionFactory.createConnection(conf);
             Table table = connection.getTable(TableName.valueOf(hbaseConfig.getTableName()))) {

            List<Put> batch = new ArrayList<>();
            iter.forEachRemaining(batch::add);
            table.put(batch);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
