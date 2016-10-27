package com.epam.bigdata2016fp.sparkstreaming.conf;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.io.Serializable;
import java.util.Map;

@ConfigurationProperties
public class AppProperties implements Serializable {

    private KafkaConnection kafkaConnection = new KafkaConnection();
    private Spark spark = new Spark();
    private Hbase hbase = new Hbase();
    private Hadoop hadoop = new Hadoop();
    private ElasticSearch elasticSearch = new ElasticSearch();

    public KafkaConnection getKafkaConnection() {
        return kafkaConnection;
    }

    public Spark getSpark() {
        return spark;
    }

    public Hbase getHbase() {
        return hbase;
    }

    public Hadoop getHadoop() {
        return hadoop;
    }

    public ElasticSearch getElasticSearch() {
        return elasticSearch;
    }

    public static class KafkaConnection implements Serializable {
        private String zookeeper;
        private String group;
        private Map<String, Integer> topics;

        public String getZookeeper() {
            return zookeeper;
        }

        public void setZookeeper(String zookeeper) {
            this.zookeeper = zookeeper;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public Map<String, Integer> getTopics() {
            return topics;
        }

        public void setTopics(Map<String, Integer> topics) {
            this.topics = topics;
        }

        @Override
        public String toString() {
            return "KafkaConnection{" +
                    "zookeeper='" + zookeeper + '\'' +
                    ", group='" + group + '\'' +
                    ", topics=" + topics +
                    '}';
        }
    }

    public static class Spark implements Serializable {
        private String appName;
        private int duration;

        public String getAppName() {
            return appName;
        }

        public void setAppName(String appName) {
            this.appName = appName;
        }

        public int getDuration() {
            return duration;
        }

        public void setDuration(int duration) {
            this.duration = duration;
        }

        @Override
        public String toString() {
            return "Spark{" +
                    "appName='" + appName + '\'' +
                    ", duration=" + duration +
                    '}';
        }
    }

    public static class Hbase implements Serializable {
        private String tableName;
        private String columnFamily;
        private String zookeeperClientPort;
        private String zookeeperQuorum;
        private String zookeeperParent;

        public String getTableName() {
            return tableName;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public void setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
        }

        public String getZookeeperClientPort() {
            return zookeeperClientPort;
        }

        public void setZookeeperClientPort(String zookeeperClientPort) {
            this.zookeeperClientPort = zookeeperClientPort;
        }

        public String getZookeeperQuorum() {
            return zookeeperQuorum;
        }

        public void setZookeeperQuorum(String zookeeperQuorum) {
            this.zookeeperQuorum = zookeeperQuorum;
        }

        public String getZookeeperParent() {
            return zookeeperParent;
        }

        public void setZookeeperParent(String zookeeperParent) {
            this.zookeeperParent = zookeeperParent;
        }

        @Override
        public String toString() {
            return "Hbase{" +
                    "tableName='" + tableName + '\'' +
                    ", zookeeperClientPort='" + zookeeperClientPort + '\'' +
                    ", zookeeperQuorum='" + zookeeperQuorum + '\'' +
                    ", zookeeperParent='" + zookeeperParent + '\'' +
                    '}';
        }
    }

    public static class Hadoop implements Serializable {
        private String fileSystem;
        private String cityDictionary;

        public String getFileSystem() {
            return fileSystem;
        }

        public void setFileSystem(String fileSystem) {
            this.fileSystem = fileSystem;
        }

        public String getCityDictionary() {
            return cityDictionary;
        }

        public void setCityDictionary(String cityDictionary) {
            this.cityDictionary = cityDictionary;
        }
    }

    public static class ElasticSearch implements Serializable {
        private String index;
        private String type;

        public String getIndex() {
            return index;
        }

        public void setIndex(String index) {
            this.index = index;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }

    @Override
    public String toString() {
        return "AppProperties{" +
                kafkaConnection +
                ", =" + spark +
                ", =" + hbase +
                '}';
    }
}