package com.kafka.sales.model;

public class SalesTotalData {
    private int franchise_id;
    private String store_brand;
    private int store_count;
    private long total_sales;
    private String update_time;

    // Default constructor
    public SalesTotalData() {}

    // Constructor with all fields
    public SalesTotalData(int franchise_id, String store_brand, int store_count, long total_sales, String update_time) {
        this.franchise_id = franchise_id;
        this.store_brand = store_brand;
        this.store_count = store_count;
        this.total_sales = total_sales;
        this.update_time = update_time;
    }

    // Getters and Setters
    public int getFranchise_id() {
        return franchise_id;
    }

    public void setFranchise_id(int franchise_id) {
        this.franchise_id = franchise_id;
    }

    public String getStore_brand() {
        return store_brand;
    }

    public void setStore_brand(String store_brand) {
        this.store_brand = store_brand;
    }

    public int getStore_count() {
        return store_count;
    }

    public void setStore_count(int store_count) {
        this.store_count = store_count;
    }

    public long getTotal_sales() {
        return total_sales;
    }

    public void setTotal_sales(long total_sales) {
        this.total_sales = total_sales;
    }

    public String getUpdate_time() {
        return update_time;
    }

    public void setUpdate_time(String update_time) {
        this.update_time = update_time;
    }
}
