package com.epam.bigdata2016fp.sparkstreaming.model;

import java.io.Serializable;

public class CityInfo implements Serializable {
    private float latitude;
    private float longitude;

    public static CityInfo parseLine(String line) {
        CityInfo info = new CityInfo();
        String[] params = line.split("\\t");
        info.latitude = Float.parseFloat(params[6]);
        info.longitude = Float.parseFloat(params[7]);
        return info;
    }
}