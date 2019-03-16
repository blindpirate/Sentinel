package com.alibaba.csp.sentinel.fixture;

import com.alibaba.csp.sentinel.util.AssertUtil;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.concurrent.atomic.AtomicInteger;

public class RetryRule implements TestRule {
    private AtomicInteger retryCount = new AtomicInteger(0);

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Retry annotation = description.getAnnotation(Retry.class);
                if (annotation == null) {
                    base.evaluate();
                } else {
                    int maxCount = annotation.maxCount();

                    AssertUtil.isTrue(maxCount > 1, "Max retry count must be >1");

                    Throwable caughtThrowable = null;
                    while (true) {
                        if (retryCount.incrementAndGet() > maxCount) {
                            throw caughtThrowable;
                        } else {
                            try {
                                base.evaluate();
                                return;
                            } catch (Throwable t) {
                                caughtThrowable = t;
                            }
                        }
                    }
                }
            }
        };
    }
}
