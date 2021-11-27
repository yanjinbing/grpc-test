package org.example;

import com.alipay.sofa.jraft.Lifecycle;
import com.alipay.sofa.jraft.entity.LogEntry;
import com.alipay.sofa.jraft.option.LogStorageOptions;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.storage.impl.RocksDBLogStorage;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class LogStorageImpl extends RocksDBLogStorage {

    private boolean closeLog = true;
    private AtomicInteger logIndex = new AtomicInteger(1);
    private LogEntry logEntry;

    public LogStorageImpl(String path, RaftOptions raftOptions) {
        super(path, raftOptions);
    }


    /**
     * Returns first log index in log.
     */
    @Override
    public long getFirstLogIndex() {
        long l =  closeLog ? logIndex.get() : super.getFirstLogIndex();
        System.out.println("LogStorage getFirstLogIndex " + l);
        return l;
    }

    /**
     * Returns last log index in log.
     */
    public long getLastLogIndex() {
        long l = closeLog ? logIndex.get()-1 : super.getLastLogIndex();
        System.out.println("LogStorage getLastLogIndex " + l);
        return l;
    }

    /**
     * Get logEntry by index.
     */
    public LogEntry getEntry(final long index) {
        System.out.println("LogStorage getEntry " + index);
        LogEntry entry = closeLog ? logEntry : super.getEntry(index);
        return entry;
    }

    /**
     * Get logEntry's term by index. This method is deprecated, you should use {@link #getEntry(long)} to get the log id's term.
     *
     * @deprecated
     */
    @Deprecated
    public long getTerm(final long index) {
        long l = closeLog ? logEntry.getId().getTerm() : super.getTerm(index);
        System.out.println("LogStorage getTerm " + l);
        return l;
    }

    /**
     * Append entries to log.
     */
    public boolean appendEntry(final LogEntry entry) {
        System.out.println("LogStorage appendEntry " + entry.getId());
        if ( closeLog ){
            logEntry = entry;
            return true;
        }

        return super.appendEntry(entry);
    }

    /**
     * Append entries to log, return append success number.
     */
    public int appendEntries(final List<LogEntry> entries) {
        if (closeLog) {
            logEntry = entries.get(entries.size() - 1);
            logIndex.addAndGet(entries.size());
        }
        int num =  super.appendEntries(entries);
        System.out.println("LogStorage appendEntries " + entries.size() + " " + num);
        return num;
    }

    /**
     * Delete logs from storage's head, [first_log_index, first_index_kept) will
     * be discarded.
     */
    public boolean truncatePrefix(final long firstIndexKept) {
        System.out.println("LogStorage truncatePrefix " + firstIndexKept);
        return closeLog ? true : super.truncatePrefix(firstIndexKept);
    }

    /**
     * Delete uncommitted logs from storage's tail, (last_index_kept, last_log_index]
     * will be discarded.
     */
    public boolean truncateSuffix(final long lastIndexKept) {
        System.out.println("LogStorage truncateSuffix " + lastIndexKept);
        return closeLog ? true : super.truncateSuffix(lastIndexKept);
    }

    /**
     * Drop all the existing logs and reset next log index to |next_log_index|.
     * This function is called after installing snapshot from leader.
     */
    public boolean reset(final long nextLogIndex) {
        System.out.println("LogStorage reset " + nextLogIndex );
        return closeLog ? true : super.reset(nextLogIndex);
    }
}
