package com.qf.gp1922.day04;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadDemo {
    public static void main(String[] args) throws Exception {
        // 创建一个固定线程数量的线程池, 需要指定线程数量的上限
//        ExecutorService threadPool = Executors.newFixedThreadPool(3);
        // 创建一个可复用线程的线程池
        ExecutorService threadPool = Executors.newCachedThreadPool();

//        threadPool.execute(new Runnable() {
//            @Override
//            public void run() {
//                System.out.println("Tread name:" + Thread.currentThread().getName() +
//                        ", Thread id:" + Thread.currentThread().getId());
//
//                try {
//                    Thread.sleep(2000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        });

//        threadPool.shutdown();

        for (int i = 0; i < 10; i++) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Tread name:" + Thread.currentThread().getName() +
                            ", Thread id:" + Thread.currentThread().getId());

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        System.out.println(Thread.currentThread().getName());
        Thread.sleep(2000);

        for (int i = 0; i < 15; i++) {
            threadPool.execute(new Runnable() {
                @Override
                public void run() {
                    System.out.println("Tread name:" + Thread.currentThread().getName() +
                            ", Thread id:" + Thread.currentThread().getId());

                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }


    }
}
