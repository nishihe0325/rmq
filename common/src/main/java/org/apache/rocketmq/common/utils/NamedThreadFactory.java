package org.apache.rocketmq.common.utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {
    private final String baseName;
    private final AtomicInteger threadNum = new AtomicInteger(0);

    public NamedThreadFactory(String baseName) {
        this.baseName = baseName;
    }

    public Thread newThread(Runnable r) {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setName(baseName + "-" + threadNum.getAndIncrement());
        return t;
    }

}
