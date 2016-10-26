package com.epam.bigdata2016fp.sparkstreaming.helper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class FileHelper {

    private static Configuration conf;
    private static FileSystem fileSystem;
    private static final String FILE_DESTINATION = "hdfs://sandbox.hortonworks.com:8020/";

    public static void initFileHelper() throws URISyntaxException, IOException {
        conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        fileSystem = FileSystem.get(new URI("hdfs://sandbox.hortonworks.com:8020"), conf);
    }

    public static List<String> getLinesFromFile(String filePath) throws URISyntaxException, IOException {
        List<String> lines = new ArrayList<>();
        initFileHelper();
        Path path = new Path(FILE_DESTINATION + filePath);
        BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(path)));

        String line = br.readLine();
        String topLine = line;
        System.out.println(topLine);
        line = br.readLine();
        while (line != null) {
            lines.add(line);
            line = br.readLine();
        }
        return lines;
    }

}