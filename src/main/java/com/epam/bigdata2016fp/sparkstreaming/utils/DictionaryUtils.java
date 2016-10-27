package com.epam.bigdata2016fp.sparkstreaming.utils;

import com.epam.bigdata2016fp.sparkstreaming.conf.AppProperties;
import com.epam.bigdata2016fp.sparkstreaming.model.CityInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.stream.Collectors;

public class DictionaryUtils {

    public static Map<String, CityInfo> citiesDictionry(AppProperties.Hadoop hadoopConf) {
        BufferedReader br=null;
        try {
            FileSystem fs = FileSystem.get(new URI(hadoopConf.getFileSystem()), new Configuration());
            br = new BufferedReader(new InputStreamReader(fs.open(new Path(hadoopConf.getCityDictionary()))));
            return br.lines()
                    .skip(1)
                    .collect(
                            Collectors.toMap(
                                    line -> line.split("\\t")[0],  // key -id
                                    CityInfo::parseLine)           //value - CityInfo
                    );

        } catch (URISyntaxException | IOException ex) {
            throw new RuntimeException(ex);
        }
        finally {
            try {
                if(br!=null)br.close();
            }catch (Exception ex){
                throw new RuntimeException(ex);
            }
        }
    }

    public static void tagsDictionary() {

    }

}