package com.github.krish.kafka.streams.model;

public class Farm {
    private String farmerName;
    private String farmerMobilr;

    public String getFarmerName() {
        return farmerName;
    }

    public void setFarmerName(String farmerName) {
        this.farmerName = farmerName;
    }

    public String getFarmerMobilr() {
        return farmerMobilr;
    }

    public void setFarmerMobilr(String farmerMobilr) {
        this.farmerMobilr = farmerMobilr;
    }

    public String getFarmerId() {
        return farmerId;
    }

    public void setFarmerId(String farmerId) {
        this.farmerId = farmerId;
    }

    private String farmerId;
}
