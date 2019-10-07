package com.qf.gp1922.day19.redisdemo.setdemo;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.Set;

public class LolBoxViewer {
    private static Jedis jedis = new Jedis("node02", 6379);

    public static void main(String[] args) throws Exception {
        // 计数器
        int i = 1;

        while (true) {
            Thread.sleep(3000);
            System.out.println("第" + i + "次查看榜单");
            // 从redis获取榜单排名信息，取5名
            Set<Tuple> heros = jedis.zrevrangeWithScores("hero:ccl", 0, 4);
            for (Tuple hero: heros) {
                System.out.println(hero.getElement() + "---------------" + hero.getScore());
            }
            i ++;

            System.out.println("");
        }

    }
}
