package com.epam.bigdata2016fp.sparkstreaming.model;

import java.io.Serializable;

public class CityInfo implements Serializable {
    private float lat;
    private float lon;

    public static CityInfo parseLine(String line) {
        CityInfo info = new CityInfo();
        String[] params = line.split("\\t");
        info.lat = Float.parseFloat(params[6]);
        info.lon = Float.parseFloat(params[7]);
        return info;
    }

    public float getLat() {
        return lat;
    }

    public void setLat(float lat) {
        this.lat = lat;
    }

    public float getLon() {
        return lon;
    }

    public void setLon(float lon) {
        this.lon = lon;
    }

    public String toString() {
        return "CityInfo{" +
                "lat=" + lat +
                ", long=" + lon +
                '}';
    }
}