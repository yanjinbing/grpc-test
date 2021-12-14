package org.example.grpc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ScheduleTest {

    public static void main(String[] args) throws InterruptedException {
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                System.out.println(" " + Thread.currentThread().getId() + " " + System.currentTimeMillis());

            }
        }, 1, 1, TimeUnit.SECONDS);

        Thread.sleep(30000);
    }

}
