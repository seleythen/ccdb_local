package ch.alice.o2.ccdb.testing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import ch.alice.o2.ccdb.servlets.SQLObject;

public class SQLInsertWithGivenRate {
    /**
     * @param args
     * @throws InterruptedException
     */
    private static final Monitor monitor = MonitorFactory.getMonitor(SQLInsertWithGivenRate.class.getCanonicalName());

    public static void main(final String[] args) throws InterruptedException {
        final long targetNoOfObjects = args.length >= 1 ? Long.parseLong(args[0]) : 100000;

        final int noThreads = args.length >= 2 ? Integer.parseInt(args[1]) : 10;

        long sleepTime = args.length >= 3 ? Long.parseLong(args[2]) : 10;

        long startingCount = 0;

        final long base = startingCount;

        final long noOfObjects = (targetNoOfObjects > startingCount && noThreads > 0)
                ? ((targetNoOfObjects - startingCount) / noThreads)
                : 0;

        System.err.println("Inserting " + (noOfObjects * noThreads) + " new objects in the database on " + noThreads
                + " threads, starting from " + base + " (target = " + targetNoOfObjects + " objects)");

        AtomicInteger insertedObjects = new AtomicInteger(0);

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(noThreads);

        final Runnable insertTask = new Runnable() {
            @Override
            public void run() {
                final SQLObject obj = new SQLObject("dummy");

                obj.validFrom = System.currentTimeMillis();
                obj.validUntil = obj.validFrom + 6000;

                obj.fileName = "some_new_detector_object.root";
                obj.contentType = "application/octet-stream";
                obj.uploadedFrom = "127.0.0.1";
                obj.size = base + noOfObjects;
                obj.md5 = "7e8fbee4f76f7079ec87bdc83d7d5538";

                obj.replicas.add(Integer.valueOf(1));

                obj.save(null);

                insertedObjects.incrementAndGet();
            }
        };

        final ScheduledFuture<?> insertHandle = scheduler.scheduleAtFixedRate(insertTask, 0, sleepTime, MICROSECONDS);

        Timer timer = new Timer();
        int interval = 1000;
        timer.schedule(new TimerTask() {
            int previouslyInserted = 0;

            @Override
            public void run() {
                // call the method
                int nowInserted = insertedObjects.get();
                monitor.addMeasurement("Inserted objects", nowInserted - previouslyInserted);
                previouslyInserted = nowInserted;

                if (nowInserted >= targetNoOfObjects) {
                    timer.cancel();
                    insertHandle.cancel(false);
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {

                    } finally {
                        System.exit(0);
                    }

                }
            }
        }, interval, interval);
        while (insertedObjects.get() < targetNoOfObjects) {
            Thread.sleep(1000);
        }
        System.exit(0);
    }
}
