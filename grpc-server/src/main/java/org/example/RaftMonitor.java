package org.example;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

/**
 * Raft健康监控，nodeOptions.setEnableMetrics(true)才有效
 * 监控NodeImpl.applyQueue、LogManagerImpl.diskQueue、FSMCallerImpl.taskQueue三个队列使用情况
 */
public class RaftMonitor {

    public enum Level {
        HEALTHY(0, 1.0),
        THIRTY(1, 0.3),
        TWENTY(2, 0.2),
        DANGER(10, 0.1);
        private int value;
        private double ratio;
        Level(int value, double ratio) {
            this.value = value;
            this.ratio = ratio;
        }

        public int getValue() {
            return value;
        }
        public double getRatio() {
            return ratio;
        }
    }

    static class Pair<K, V> {
        private K key;
        private V value;
        public V getValue() { return value; }
        public void setValue(V value){ this.value = value;}
        public K getKey() { return key; }
        public void setKey(K key){ this.key = key; }
    }

    final Level[] levelsOrder = new Level[]{Level.DANGER, Level.TWENTY, Level.THIRTY, Level.HEALTHY};

    // NodeImpl.applyQueue, k 最大值，v 可用值
    private Pair<Gauge<Integer>, Gauge<Long>> nodeApplyQueue;
    // LogManagerImpl.diskQueue
    private Pair<Gauge<Integer>, Gauge<Long>> logDiskQueue;
    // FSMCallerImpl.taskQueue
    private Pair<Gauge<Integer>, Gauge<Long>> fsmTaskQueue;

    private boolean available;
    final private int groupId;
    private Level currentLevel;
    private Level lastLevel;



    public RaftMonitor(MetricRegistry metricRegistry, int groupId) {
        this.groupId = groupId;
        available = false;
        if (metricRegistry != null) {
            nodeApplyQueue = new Pair<>();
            logDiskQueue = new Pair<>();
            fsmTaskQueue = new Pair<>();
            metricRegistry.getGauges().forEach((k, v) -> {
                if (k.equalsIgnoreCase("jraft-fsm-caller-disruptor.buffer-size"))
                    fsmTaskQueue.setKey(v);
                else if (k.equalsIgnoreCase("jraft-fsm-caller-disruptor.remaining-capacity"))
                    fsmTaskQueue.setValue(v);
                else if (k.equalsIgnoreCase("jraft-log-manager-disruptor.buffer-size"))
                    logDiskQueue.setKey(v);
                else if (k.equalsIgnoreCase("jraft-log-manager-disruptor.remaining-capacity"))
                    logDiskQueue.setValue(v);
                else if (k.equalsIgnoreCase("jraft-node-impl-disruptor.buffer-size"))
                    nodeApplyQueue.setKey(v);
                else if (k.equalsIgnoreCase("jraft-node-impl-disruptor.remaining-capacity"))
                    nodeApplyQueue.setValue(v);
            });
            available = true;
        }
        currentLevel = Level.HEALTHY;
    }

    public Level getLevel(){
        lastLevel = currentLevel;
        if ( available ) {
            try {
                double fsm = (double) fsmTaskQueue.getValue().getValue() / fsmTaskQueue.getKey().getValue();
                double log = (double) logDiskQueue.getValue().getValue() / logDiskQueue.getKey().getValue();
                double node = (double) nodeApplyQueue.getValue().getValue() / nodeApplyQueue.getKey().getValue();

                for (Level level : levelsOrder) {
                    if (fsm < level.getRatio() || log < level.getRatio() || node < level.getRatio()) {
                        currentLevel = level;
                        break;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();

            }
        }
        return currentLevel;
    }

    public Level getLastLevel() { return  lastLevel;}
    public String toString(){
        return String.format("Fsm taskQueue %d-%d, LogManager diskQueue %d-%d, Node applyQueue %d-%d， level is %s",
                fsmTaskQueue.getKey().getValue(), fsmTaskQueue.getValue().getValue(),
                logDiskQueue.getKey().getValue(), logDiskQueue.getValue().getValue(),
                nodeApplyQueue.getKey().getValue(), nodeApplyQueue.getValue().getValue(),
                currentLevel);

    }
}
