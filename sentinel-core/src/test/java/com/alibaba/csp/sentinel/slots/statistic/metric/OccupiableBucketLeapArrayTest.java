package com.alibaba.csp.sentinel.slots.statistic.metric;

import com.alibaba.csp.sentinel.fixture.Retry;
import com.alibaba.csp.sentinel.fixture.RetryRule;
import com.alibaba.csp.sentinel.slots.statistic.base.WindowWrap;
import com.alibaba.csp.sentinel.slots.statistic.data.MetricBucket;
import com.alibaba.csp.sentinel.slots.statistic.metric.occupy.OccupiableBucketLeapArray;
import com.alibaba.csp.sentinel.util.TimeUtil;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for {@link OccupiableBucketLeapArray}.
 *
 * @author jialiang.linjl
 */
public class OccupiableBucketLeapArrayTest {
    @Rule
    public RetryRule retryRule = new RetryRule();

    private final int windowLengthInMs = 200;
    private final int intervalInSec = 2;
    private final int intervalInMs = intervalInSec * 1000;
    private final int sampleCount = intervalInMs / windowLengthInMs;

    @Test
    @Retry
    public void testNewWindow() {
        long currentTime = TimeUtil.currentTimeMillis();
        OccupiableBucketLeapArray leapArray = new OccupiableBucketLeapArray(sampleCount, intervalInMs);

        WindowWrap<MetricBucket> currentWindow = leapArray.currentWindow(currentTime);
        currentWindow.value().addPass(1);
        assertEquals(currentWindow.value().pass(), 1L);

        leapArray.addWaiting(currentTime + windowLengthInMs, 1);
        assertEquals(leapArray.currentWaiting(), 1);
        assertEquals(currentWindow.value().pass(), 1L);

    }

    @Test
    @Retry
    public void testWindowInOneInterval() {
        OccupiableBucketLeapArray leapArray = new OccupiableBucketLeapArray(sampleCount, intervalInMs);
        long currentTime = TimeUtil.currentTimeMillis();

        WindowWrap<MetricBucket> currentWindow = leapArray.currentWindow(currentTime);
        currentWindow.value().addPass(1);
        assertEquals(currentWindow.value().pass(), 1L);

        leapArray.addWaiting(currentTime + windowLengthInMs, 2);
        assertEquals(leapArray.currentWaiting(), 2);
        assertEquals(currentWindow.value().pass(), 1L);

        leapArray.currentWindow(currentTime + windowLengthInMs);
        List<MetricBucket> values = leapArray.values(currentTime + windowLengthInMs);
        assertEquals(values.size(), 2);

        long sum = 0;
        for (MetricBucket bucket : values) {
            sum += bucket.pass();
        }
        assertEquals(sum, 3);
    }

    @Test
    @Retry
    public void testMultiThreadUpdateEmptyWindow() throws Exception {
        final long time = TimeUtil.currentTimeMillis();
        final int nThreads = 16;
        final OccupiableBucketLeapArray leapArray = new OccupiableBucketLeapArray(sampleCount, intervalInMs);
        final CountDownLatch latch = new CountDownLatch(nThreads);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                leapArray.currentWindow(time).value().addPass(1);
                leapArray.addWaiting(time + windowLengthInMs, 1);
                latch.countDown();
            }
        };

        for (int i = 0; i < nThreads; i++) {
            new Thread(task).start();
        }

        latch.await();

        assertEquals(nThreads, leapArray.currentWindow(time).value().pass());
        assertEquals(nThreads, leapArray.currentWaiting());

        leapArray.currentWindow(time + windowLengthInMs);
        long sum = 0;
        List<MetricBucket> values = leapArray.values(time + windowLengthInMs);
        for (MetricBucket bucket : values) {
            sum += bucket.pass();
        }
        assertEquals(values.size(), 2);
        assertEquals(sum, nThreads * 2);
    }

    @Test
    @Retry
    public void testWindowAfterOneInterval() {
        OccupiableBucketLeapArray leapArray = new OccupiableBucketLeapArray(sampleCount, intervalInMs);
        long currentTime = TimeUtil.currentTimeMillis();

        System.out.println(currentTime);
        for (int i = 0; i < intervalInSec * 1000 / windowLengthInMs; i++) {
            WindowWrap<MetricBucket> currentWindow = leapArray.currentWindow(currentTime + i * windowLengthInMs);
            currentWindow.value().addPass(1);
            leapArray.addWaiting(currentTime + (i + 1) * windowLengthInMs, 1);
            System.out.println(currentTime + i * windowLengthInMs);
            leapArray.debug(currentTime + i * windowLengthInMs);
        }

        System.out.println(currentTime + intervalInSec * 1000);
        List<MetricBucket> values = leapArray
            .values(currentTime - currentTime % windowLengthInMs + intervalInSec * 1000);
        leapArray.debug(currentTime + intervalInSec * 1000);
        assertEquals(values.size(), intervalInSec * 1000 / windowLengthInMs);

        long sum = 0;
        for (MetricBucket bucket : values) {
            sum += bucket.pass();
        }
        assertEquals(sum, 2 * intervalInSec * 1000 / windowLengthInMs - 1);
        assertEquals(leapArray.currentWaiting(), 10);
    }
}
