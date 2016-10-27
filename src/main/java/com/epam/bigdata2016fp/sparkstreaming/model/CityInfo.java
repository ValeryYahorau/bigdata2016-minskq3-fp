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

    public float getLatitude() {
        return lat;
    }

    public void setLatitude(float latitude) {
        this.lat = latitude;
    }

    public float getLongitude() {
        return lon;
    }

    public void setLongitude(float longitude) {
        this.lon = longitude;
    }

    @Override
    public String toString() {
        return "CityInfo{" +
                "latitude=" + lat +
                ", longitude=" + lon +
                '}';
    }
}