package com.example.rocksdb;

import net.openhft.chronicle.map.ChronicleMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;

import java.util.Set;
import java.util.concurrent.*;

public class Test {

    public static void main(String[] args) throws Exception {
        int num = 16;

        CountDownLatch latch = new CountDownLatch(num);
        ConcurrentSet set = new ConcurrentSet();
        long start = System.currentTimeMillis();
        for (int j = 0; j < num; j++) {
            new Thread(() -> {
                for (int i = 0; i < 100000000 / num; i++)
                    set.contains("");
                latch.countDown();
            }).start();
        }
        latch.await();
        ;
        System.out.println("time is " + (System.currentTimeMillis() - start) + " " + set.getCounter());

    }

    public static void main3(String[] args) throws Exception {
        long total = Long.valueOf(args[0]);
        int times = 64;
        long oneTotal = total / times;
        while (true) {
            long start = System.currentTimeMillis();
            ChronicleMap map = ChronicleMap.of(Long.class, Long.class)
                    .name("map")
                    .entries(total)
                    .create();

            map.put(1L, 1L);
            long start2 = System.currentTimeMillis();

            CountDownLatch latch = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                long t = oneTotal * i;
                new Thread(() -> {
                    for (long l = 0; l < oneTotal; l++) {
                        if (!map.containsKey((Long) t + l))
                            map.put((Long) t + l, (Long) t + l);
                    }
                    latch.countDown();
                }).start();
            }
            latch.await();
            System.out.println("time is " + (System.currentTimeMillis() - start)
                    + " " + (System.currentTimeMillis() - start2) + " " + map.size());
            System.out.println("Press any key");
            System.in.read();

            map.clear();
            map.close();
            System.gc();
        }


    }

    public static void main2(String[] args) throws Exception {
        long total = Long.valueOf(args[0]);
        int times = 64;
        long oneTotal = total / times;
        while (true) {
            long start = System.currentTimeMillis();


            Set<Long> sets = ConcurrentHashMap.newKeySet(Integer.valueOf(args[1]));
            sets.add(1L);
            long start2 = System.currentTimeMillis();

            CountDownLatch latch = new CountDownLatch(times);
            for (int i = 0; i < times; i++) {
                long t = oneTotal * i;
                Set<Long> finalSets = sets;
                new Thread(() -> {
                    for (long l = 0; l < oneTotal; l++) {
                        finalSets.add(t + l);
                    }
                    latch.countDown();
                }).start();
            }
            latch.await();
            System.out.println("time is " + (System.currentTimeMillis() - start)
                    + " " + (System.currentTimeMillis() - start2) + " " + sets.size());
            System.out.println("Press any key");
            System.in.read();

            sets.clear();
            sets = null;
            System.gc();
        }


    }
}
