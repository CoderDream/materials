package com.qf.gp1922.day04;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class FutureDemo {
    public static void main(String[] args) throws Exception {
        final ExecutorService pool = Executors.newCachedThreadPool();
        /**
         * 该方法有返回值，返回一个Future，Future相当于是一个容器，可以封装返回值
         * 当计算过程中没有完成，此时会线程阻塞（等待），Future在线程阻塞的时候为空值
         */
        Future<String> future = pool.submit(new Callable<String>() {
            @Override
            public String call() throws Exception {
                System.out.println("Tread name:" + Thread.currentThread().getName() +
                        ", Thread id:" + Thread.currentThread().getId());
                System.out.println("正在读取数据。。。");
                Thread.sleep(1000);
                System.out.println("读取数据完毕");
                return "success";
            }
        });

        System.out.println(Thread.currentThread().getName());
        Thread.sleep(2000);

        if (future.isDone()) {
            System.out.println(future.get());
        }else {
            System.out.println("None");
        }


    }
}
