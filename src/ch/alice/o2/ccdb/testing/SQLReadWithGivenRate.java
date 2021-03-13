package ch.alice.o2.ccdb.testing;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import alien.monitoring.Monitor;
import alien.monitoring.MonitorFactory;
import ch.alice.o2.ccdb.RequestParser;
import ch.alice.o2.ccdb.servlets.SQLObject;

public class SQLReadWithGivenRate {
    private static final Monitor monitor = MonitorFactory.getMonitor(SQLReadWithGivenRate.class.getCanonicalName());

    public static void main(final String[] args) throws InterruptedException {
        final int noThreads = args.length >= 1 ? Integer.parseInt(args[0]) : 10;

        final int noSchedulers = args.length >= 2 ? Integer.parseInt(args[1]) : 10;

        long sleepTime = args.length >= 2 ? Long.parseLong(args[2]) : 10;

        AtomicInteger[] readObjectsArr = new AtomicInteger[noSchedulers];
        AtomicInteger[] nullObjectsArr = new AtomicInteger[noSchedulers];
        for(int i = 0;i < noSchedulers;i++) {
            readObjectsArr[i] = new AtomicInteger(0);
            nullObjectsArr[i] = new AtomicInteger(0);
        }
       

        final Random r = new Random(System.currentTimeMillis());

        for (int i = 0; i < noSchedulers; i++) {
            AtomicInteger readObjects = readObjectsArr[i];
            AtomicInteger nullObjects = nullObjectsArr[i];
            readObjects.set(0);
            nullObjects.set(0);
            new Thread(() -> {
                final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(noThreads);
                scheduler.scheduleAtFixedRate(() -> {
                    final RequestParser parser = new RequestParser(null);

                    parser.path = "dummy";
                    // parser.startTime = i * rangeWidth + r.nextLong() % rangeWidth;
                    parser.startTime = System.currentTimeMillis() - 10 - Math.abs(r.nextLong() % 1000000);
                    parser.startTimeSet = true;

                    final SQLObject result = SQLObject.getMatchingObject(parser);

                    if (result == null)
                        nullObjects.incrementAndGet();
                    else
                        readObjects.incrementAndGet();
                }, 0, sleepTime, MICROSECONDS);
            }).start();
        }

        Timer timer = new Timer();
        int interval = 1000;
        timer.schedule(new TimerTask() {
            int previousReadRequests = 0;
            int previousNull = 0;
            int previousSuccess = 0;

            @Override
            public void run() {
                // call the method

                int nulls = 0;
                for(int i = 0;i < noSchedulers;i++) {
                    nulls += nullObjectsArr[i].get();
                }
                monitor.addMeasurement("Null objects", nulls - previousNull);
                previousNull = nulls;

                int currentReadObjects = 0;
                for(int i = 0;i < noSchedulers;i++) {
                    currentReadObjects += readObjectsArr[i].get();
                }
                monitor.addMeasurement("Read objects", currentReadObjects - previousSuccess);
                previousSuccess = currentReadObjects;

                int readRequests = nulls + currentReadObjects;
                monitor.addMeasurement("Read requests", readRequests - previousReadRequests);
                previousReadRequests = readRequests;
            }
        }, interval, interval);
        while (true) {
            Thread.sleep(1000);
        }
    }
}
