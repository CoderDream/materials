package com.qf.gp1922.day19.redisdemo.setdemo;

import redis.clients.jedis.Jedis;

import java.util.Random;

public class LolBoxPlayer {
    private static Jedis jedis = new Jedis("node02", 6379);

    public static void main(String[] args) throws Exception {
        Random r = new Random();

        String[] heros = {"盖伦","轮子妈","蒙多","亚索","木木","刀妹","提莫","炼金"};

        while (true) {
            int index = r.nextInt(heros.length);
            String hero = heros[index];
            Thread.sleep(500);
            // 给英雄每次出场的分数加1，如果初次出场，zincrby方法会自动创建
            jedis.zincrby("hero:ccl", 1, hero);
            System.out.println(hero + "出场了");
        }

    }
}
