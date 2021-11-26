package org.example;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;

import java.util.List;

public class LogStorageImpl extends RocksDBLogStorage {
    public LogStorageImpl(String path, RaftOptions raftOptions) {
        super(path, raftOptions);
    }


    /**
     * Returns first log index in log.
     */
    @Override
    public long getFirstLogIndex() {
        long l =  super.getFirstLogIndex();
        System.out.println("getFirstLogIndex " + l);
        return l;
    }

    /**
     * Returns last log index in log.
     */
    public long getLastLogIndex() {
        long l = super.getLastLogIndex();
        System.out.println("getLastLogIndex " + l);
        return l;
    }

    /**
     * Get logEntry by index.
     */
    public LogEntry getEntry(final long index) {
        System.out.println("getEntry " + index);
        return super.getEntry(index);
    }

    /**
     * Get logEntry's term by index. This method is deprecated, you should use {@link #getEntry(long)} to get the log id's term.
     *
     * @deprecated
     */
    @Deprecated
    public long getTerm(final long index) {
        long l = super.getTerm(index);
        System.out.println("getTerm " + l);
        return l;
    }

    /**
     * Append entries to log.
     */
    public boolean appendEntry(final LogEntry entry) {
        System.out.println("appendEntry " + entry.getId());
        return super.appendEntry(entry);
    }

    /**
     * Append entries to log, return append success number.
     */
    public int appendEntries(final List<LogEntry> entries) {
        return super.appendEntries(entries);
    }

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will
     * be discarded.
     */
    public boolean truncatePrefix(final long firstIndexKept) {
        System.out.println("truncatePrefix " + firstIndexKept);
        return super.truncatePrefix(firstIndexKept);
    }

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index]
     * will be discarded.
     */
    public boolean truncateSuffix(final long lastIndexKept) {
        System.out.println("truncateSuffix " + lastIndexKept);
        return super.truncateSuffix(lastIndexKept);
    }

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|.
     * This function is called after installing snapshot from leader.
     */
    public boolean reset(final long nextLogIndex) {
        return super.reset(nextLogIndex);
    }
}
