package com.example.rocksdb;

import java.util.concurrent.*;

public class Test {
    public static CompletableFuture<Integer> compute() {
        final CompletableFuture<Integer> future = new CompletableFuture<>();
       // future.complete(10);
        return future;
    }

    public static void main(String[] args) throws Exception {
        ExecutorService executor = new ThreadPoolExecutor(10, 10,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(100));
        Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                System.out.println("T1 enter");
                try {
                    Thread.sleep(10000);
                } catch (Exception e) {
                    System.out.println("T1 InterruptedException " + e.getMessage());
                }
                System.out.println("T1 exit");
            }
        });
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("T2 enter");
                    future.get();
                    System.out.println("T2 exit");
                } catch (InterruptedException e) {
                    System.out.println("T2 InterruptedException " + e.getMessage());
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    System.out.println("T2 InterruptedException " + e.getMessage());
                    e.printStackTrace();
                } catch (CancellationException e2){
                    System.out.println("T2 CancellationException ");
                }
            }
        }).start();

        Thread.sleep(1000);
        try {
            future.cancel(true);
        }catch (Exception e){
            e.printStackTrace();
        }

        Thread.sleep(10000);
        executor.shutdown();
    }
}
