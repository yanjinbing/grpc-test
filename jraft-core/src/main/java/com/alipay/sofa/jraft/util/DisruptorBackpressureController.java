package com.alipay.sofa.jraft.util;

import com.alipay.sofa.jraft.option.RaftOptions;
import com.lmax.disruptor.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DisruptorBackpressureController {

    private static final Logger LOG                      = LoggerFactory
            .getLogger(DisruptorBackpressureController.class);

    private DisruptorMetricSet disruptorMetricSet;
    private final RingBuffer<?> ringBuffer;
    private final RaftOptions raftOptions;
    private double backpressureScore; // [0, 1] nad < 0 means no backpressure

    public DisruptorBackpressureController(RingBuffer<?> ringBuffer, RaftOptions raftOptions) {
        this.ringBuffer = ringBuffer;
        this.raftOptions = raftOptions;
        this.backpressureScore = -1;
    }

    public void setDisruptorMetricSet(DisruptorMetricSet disruptorMetricSet) {
        this.disruptorMetricSet = disruptorMetricSet;
    }

    public double checkBackpressure() {
        return this.checkBackpressure(true);
    }
    public double checkBackpressure(boolean sleepIfBackpressure) {
        int totalSize = this.ringBuffer.getBufferSize();
        long remainingCapacity = this.ringBuffer.remainingCapacity();
        double remainingRatio = remainingCapacity * 1.0D / totalSize;
        if (remainingRatio < this.raftOptions.getDisruptorBackPressureThreshold()) {
            this.backpressureScore = Math.pow((1 - remainingRatio), 3);
            if (sleepIfBackpressure) {
                int sleepingMS =
                        (int) (this.backpressureScore *
                        this.raftOptions.getDisruptorBackPressureMaxSleepMS());
                LOG.warn("thread sleep {} ms due to back pressure", sleepingMS);
                ThreadHelper.sleep(sleepingMS);
            }
        } else {
            this.backpressureScore = -1;
        }
        if (this.disruptorMetricSet != null) {
            this.disruptorMetricSet.setBackpressure(this.backpressureScore < 0 ? 0 : 1);
        }

        return this.backpressureScore;
    }
}
