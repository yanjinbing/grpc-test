package org.example.test;

import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PhantomReferenceTest {
    private static final Map<Object, String> records = new ConcurrentHashMap<>();
    private static final ReferenceQueue<TestClass> QUEUE = new ReferenceQueue<>();
    public static void main(String[] args) {
        TestClass obj = new TestClass("演示虚引用，接收GC回收通知Test");
        // 缓存虚引用对象
        PhantomReference<TestClass> phantomReference = new PhantomReference<>(obj, QUEUE);
        records.put(phantomReference, obj.getName());
        // 这个线程不断读取引用队列，当弱引用指向的对象呗回收时，该引用就会被加入到引用队列中
        new Thread(() -> {
            while (true) {
                Reference<? extends TestClass> poll = QUEUE.poll();
                if (poll != null) {
                    System.out.println("虚引用对象被jvm回收了对象 ---- " + records.get(poll));
                }
            }
        }).start();
        // 释放对象
        obj = null;
        // 垃圾回收
        System.gc();
    }

    static class TestClass {
        private String name;
        public TestClass(String name) {
            this.name = name;
        }

        public String getName(){ return name;};
    }
}
