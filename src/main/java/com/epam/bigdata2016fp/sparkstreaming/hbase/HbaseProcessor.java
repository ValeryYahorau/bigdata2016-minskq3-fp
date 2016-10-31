package com.epam.bigdata2016fp.sparkstreaming.hbase;

import com.epam.bigdata2016fp.sparkstreaming.conf.AppProperties;
import com.epam.bigdata2016fp.sparkstreaming.model.ESModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;
import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.time.temporal.ChronoUnit.DAYS;

public class HbaseProcessor implements Serializable {

    private static DateTimeFormatter dateParser = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");

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

    public static Iterator<ESModel> getUserCategory(Iterator<ESModel> iter){
        List<ESModel> models = new ArrayList<>();
        iter.forEachRemaining(models::add);
        ResultSet rset = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            java.sql.Connection con = DriverManager.getConnection("jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase-unsecure");
            for (ESModel line: models){
                LocalDate oldDate = LocalDate.parse(line.getTimestamp(), dateParser);

                PreparedStatement statement = con.prepareStatement("select LAL as LAL, COUNT(*) as CLICKS FROM (SELECT SUBSTR(TIME,0,8) as LAL FROM LOG_TABLEF3 WHERE IPINYOU_ID=?) GROUP BY LAL ORDER BY CLICKS");
                statement.setString(1, line.getiPinyouId());
                statement.executeQuery();
                rset = statement.executeQuery();

                long priorDayClicks = 0;

                while (rset.next()) {
                    LocalDate newDate = LocalDate.parse(rset.getString("LAL"), dateParser);
                    Integer numberOfClicks = rset.getInt("CLICKS");
                    System.out.println("newTime " + newDate + ": clicks " + numberOfClicks);
                    long diff = DAYS.between(newDate, oldDate);
                    if (diff == 1) priorDayClicks += numberOfClicks;
                }
                String result;

                if (priorDayClicks >= 2) result = "CurrentHighRepeat";
                else if (priorDayClicks == 1) result = "CurrentRepeat";
                else result = "New";

                line.setCategory(result);
                System.out.println("@@@1");
                System.out.println(line.toStringifyJson());
            }
        }catch (Exception ex){
            throw new RuntimeException(ex);
        }
        return models.iterator();
    }

}
