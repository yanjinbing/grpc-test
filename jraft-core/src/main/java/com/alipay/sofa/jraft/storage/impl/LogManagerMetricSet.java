/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.storage.impl;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricSet;

import java.util.HashMap;
import java.util.Map;

/**
 * Log manager metric set.
 */
public final class LogManagerMetricSet implements MetricSet {
    private long farthestFollowerLogIndex;
    private long survivalLogCount;

    public LogManagerMetricSet() {
        super();
    }

    public long getFarthestFollowerLogIndex() {
        return farthestFollowerLogIndex;
    }

    public void setFarthestFollowerLogIndex(long farthestFollowerLogIndex) {
        this.farthestFollowerLogIndex = farthestFollowerLogIndex;
    }

    public long getSurvivalLogCount() {
        return survivalLogCount;
    }

    public void setSurvivalLogCount(long survivalLogCount) {
        this.survivalLogCount = survivalLogCount;
    }

    /**
     * Return log manager metrics
     * @return log manager metrics map
     */
    @Override
    public Map<String, Metric> getMetrics() {
        final Map<String, Metric> gauges = new HashMap<>();
        gauges.put("farthest-follower-log-index", (Gauge<Long>) this::getFarthestFollowerLogIndex);
        gauges.put("survival-log-count", (Gauge<Long>) this::getSurvivalLogCount);
        return gauges;
    }
}
