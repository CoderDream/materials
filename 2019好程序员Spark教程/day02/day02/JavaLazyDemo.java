package com.qf.gp1922.day02;

/**
 * Java中，要实现延迟加载，一般会使用单例的懒汉模式去实现
 */
public class JavaLazyDemo {
    private static MyProperty prop;

    public static MyProperty getProp() {
        if (prop == null)
            prop = new MyProperty();
        System.out.println("getProperty");

        return prop;
    }



    public static void main(String[] args) {

        System.out.println(JavaLazyDemo.getProp());

    }
}

class MyProperty{}
