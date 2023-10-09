package com.sparkTutorial.sparkSql;

import java.io.Serializable;

/**
 * Created by Neha Priya on 04-10-2019.
 */
public class SFPD implements Serializable {
    private String IncidntNum;
    private String Category;
    private String Description;
    private String DayOfWeek;
    private String DateOfIncident;
    private String TimeOfIncident;
    private String PdDistrict;
    private String Resolution;
    private String Address;
    private String X;
    private String Y;
    private String Location;

    public String getIncidntNum() {
        return IncidntNum;
    }

    public void setIncidntNum(String incidntNum) {
        IncidntNum = incidntNum;
    }

    public String getCategory() {
        return Category;
    }

    public void setCategory(String category) {
        Category = category;
    }

    public String getDescription() {
        return Description;
    }

    public void setDescription(String description) {
        Description = description;
    }

    public String getDayOfWeek() {
        return DayOfWeek;
    }

    public void setDayOfWeek(String dayOfWeek) {
        DayOfWeek = dayOfWeek;
    }

    public String getDateOfIncident() {
        return DateOfIncident;
    }

    public void setDateOfIncident(String dateOfIncident) {
        DateOfIncident = dateOfIncident;
    }

    public String getTimeOfIncident() {
        return TimeOfIncident;
    }

    public void setTimeOfIncident(String timeOfIncident) {
        TimeOfIncident = timeOfIncident;
    }

    public String getPdDistrict() {
        return PdDistrict;
    }

    public void setPdDistrict(String pdDistrict) {
        PdDistrict = pdDistrict;
    }

    public String getResolution() {
        return Resolution;
    }

    public void setResolution(String resolution) {
        Resolution = resolution;
    }

    public String getAddress() {
        return Address;
    }

    public void setAddress(String address) {
        Address = address;
    }

    public String getX() {
        return X;
    }

    public void setX(String x) {
        X = x;
    }

    public String getY() {
        return Y;
    }

    public void setY(String y) {
        Y = y;
    }

    public String getLocation() {
        return Location;
    }

    public void setLocation(String location) {
        Location = location;
    }
}
