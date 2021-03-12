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

        long sleepTime = args.length >= 2 ? Long.parseLong(args[1]) : 10;


        AtomicInteger readObjects = new AtomicInteger(0);
        AtomicInteger nullObjects = new AtomicInteger(0);
        AtomicInteger requestsFinishedNumber = new AtomicInteger(0);

        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(noThreads);

        final Random r = new Random(System.currentTimeMillis());

        final Runnable insertTask = new Runnable() {
            @Override
            public void run() {
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
                requestsFinishedNumber.incrementAndGet();
            }
        };

        scheduler.scheduleAtFixedRate(insertTask, 0, sleepTime, MICROSECONDS);

        Timer timer = new Timer();
        int interval = 1000;
        timer.schedule(new TimerTask() {
            int previousReadRequests = 0;
            int previousNull = 0;
            int previousSuccess = 0;

            @Override
            public void run() {
                // call the method
                int readRequests = requestsFinishedNumber.get();
                monitor.addMeasurement("Read requests", readRequests - previousReadRequests);
                previousReadRequests = readRequests;

                int nulls = nullObjects.get();
                monitor.addMeasurement("Null objects", nulls - previousNull);
                previousNull = nulls;

                int currentReadObjects = readObjects.get();
                monitor.addMeasurement("Read objects", currentReadObjects - previousSuccess);
                previousSuccess = currentReadObjects;
            }
        }, interval, interval);
        while (true) {
            Thread.sleep(1000);
        }
    }
}
