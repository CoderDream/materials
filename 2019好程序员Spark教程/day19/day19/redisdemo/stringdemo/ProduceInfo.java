package com.qf.gp1922.day19.redisdemo.stringdemo;

import java.io.Serializable;

public class ProduceInfo implements Serializable {
    // 商品名称
    private String name;
    // 商品价格
    private Double price;
    // 商品描述
    private String procuctDesc;

    @Override
    public String toString() {
        return "商品信息：{" +
                "商品名称='" + name + '\'' +
                ", 商品价格=" + price +
                ", 商品描述='" + procuctDesc + '\'' +
                '}';
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public String getProcuctDesc() {
        return procuctDesc;
    }

    public void setProcuctDesc(String procuctDesc) {
        this.procuctDesc = procuctDesc;
    }
}
