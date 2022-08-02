package com.example.rocksdb;

import com.google.protobuf.ByteString;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.IndexType;
import org.rocksdb.LRUCache;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.example.grpc.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RocksdbTest {
    public static void main(String args[]) throws InterruptedException, IOException {
        if (args.length < 3)
            System.out.println("dbPath cmd(1|2|3) cache(0|1)");
        else {
            scanDir(args[0], args[1], args[2]);
        }
    }

    public static void scanDir(String path, String cmd, String cache) throws IOException {
        initBlockCache();
        for (File file : new File(path).listFiles()) {
            if (file.isDirectory()) {
                new Thread(() -> {
                    try {
                        while (true) {
                            System.out.println("Scan " + "  " + file.getName());
                            testScan(file.getAbsolutePath(), file.getName(), cmd, cache);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }).start();
            }
        }
        System.out.println("test continue, input 1 to exit");
        System.in.read();
    }


    static BlockBasedTableConfig tableConfig;

    public static void initBlockCache() {
        RocksDB.loadLibrary();
        LRUCache blockCache;
        blockCache = new LRUCache(blockCacheSize, 10, true);
        tableConfig = new BlockBasedTableConfig() //
                .setIndexType(IndexType.kTwoLevelIndexSearch) //
                .setPartitionFilters(true) //
                .setMetadataBlockSize(8 * SizeUnit.KB) //
                .setCacheIndexAndFilterBlocks(false) //
                .setCacheIndexAndFilterBlocksWithHighPriority(false) //
                .setPinL0FilterAndIndexBlocksInCache(false) //
                .setBlockSize(4 * SizeUnit.KB)//
                .setBlockCache(blockCache)
                .setWholeKeyFiltering(true);
    }

    static volatile AtomicLong totalCount = new AtomicLong(0);
    static volatile long start = System.currentTimeMillis();
    static long batchSize = 10_000_0000L;
    static long blockCacheSize = 10_000_000_000L;

    static AtomicLong threadTotalCount = new AtomicLong(0);
    static AtomicLong threadCount = new AtomicLong(0);

    public static void testScan(String dbPath, String dbName, String cmd, String cache) {
        int cmdValue = Integer.valueOf(cmd);
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<ColumnFamilyDescriptor>();
            List<byte[]> columnFamilyBytes = RocksDB.listColumnFamilies(new Options(), dbPath);
            if (cache.equals("1")) {
                cfOpts.setTableFormatConfig(tableConfig);
                System.out.println("Set Table config");
            }
            if (columnFamilyBytes.size() > 0) {
                for (byte[] columnFamilyByte : columnFamilyBytes) {
                    cfDescriptors.add(new ColumnFamilyDescriptor(columnFamilyByte, cfOpts));
                }
            }
            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            RocksDB.listColumnFamilies(new Options(), dbPath);
            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
                scanAll(db, columnFamilyHandles, cmdValue);
                synchronized (totalCount){
                    totalCount.wait();
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    columnFamilyHandle.close();
                }
            } // frees the db and the db options
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public static void scanAll(RocksDB db, List<ColumnFamilyHandle> columnFamilyHandles, int cmdValue) {
        threadCount.incrementAndGet();
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            try (ReadOptions readOptions = new ReadOptions()
                    .setIgnoreRangeDeletions(false);
                 RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions)) {
                iterator.seekToFirst();
                long dcount = 0;
                while (iterator.isValid()) {
                    if (cmdValue == 1) {
                        byte[] key = iterator.key();
                        if (key.length > 2)
                            dcount += Arrays.copyOfRange(key, 0, key.length - 2).length;
                        else
                            dcount += iterator.key().length;
                        dcount += iterator.value().length;
                    } else if (cmdValue == 2) {
                        byte[] key = iterator.key();
                        if (key.length > 2)
                            dcount += Arrays.copyOfRange(key, 0, key.length - 2).length;
                        else
                            dcount += iterator.key().length;
                        dcount += iterator.value().length;
                        Kv kv = Kv.newBuilder().setKey(ByteString.copyFrom(key))
                                .setValue(ByteString.copyFrom(iterator.value()))
                                .build();
                        dcount += kv.getCode();
                    } else if (cmdValue == 3) {
                        byte[] key = iterator.key();
                        if (key.length > 2)
                            dcount += Arrays.copyOfRange(key, 0, key.length - 2).length;
                        else
                            dcount += iterator.key().length;
                        dcount += iterator.value().length;
                        Kv kv = Kv.newBuilder().setKey(ByteString.copyFrom(key))
                                .setValue(ByteString.copyFrom(iterator.value()))
                                .build();
                        dcount += kv.toByteArray().length;
                    } else {
                        System.out.println("cmd error");
                        return;
                    }
                    iterator.next();
                    if (totalCount.incrementAndGet() >= batchSize) {
                        synchronized (totalCount) {
                            long count = totalCount.get();
                            if (count > batchSize) {
                                System.out.println(totalCount + "  qps is " + count * 1000 / (System.currentTimeMillis() - start + 1)
                                        + " thread = " + threadCount.get()
                                        + " data = " + dcount);
                                totalCount.set(0);
                                start = System.currentTimeMillis();
                            }
                        }
                    }
                }
            }
        }
        threadCount.decrementAndGet();
        System.out.println(totalCount + "  qps is " + totalCount.get() * 1000 / (System.currentTimeMillis() - start + 1));

        if ( threadTotalCount.get() < 120) {
            new Thread(() -> {
                System.out.println(" " + threadTotalCount.incrementAndGet() + " thread start");
                try {
                    scanAll(db, columnFamilyHandles, cmdValue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
            new Thread(() -> {
                System.out.println(" " + threadTotalCount.incrementAndGet() + " thread start");
                try {
                    scanAll(db, columnFamilyHandles, cmdValue);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
    }
}


