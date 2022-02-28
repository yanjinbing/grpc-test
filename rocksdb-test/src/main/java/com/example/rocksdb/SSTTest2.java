package com.example.rocksdb;

import org.apache.commons.codec.binary.Hex;
import org.rocksdb.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Rocksdb 压力测试，测试多个rocksdb实例对内存的消耗
 */
public class SSTTest2 {
    static int kNumInternalBytes = 8;      //internal key 增加的8个字节后缀
    private static byte[] value;

    static {
        value = new byte[1024];
        for (int i = 0; i < value.length; i++)
            value[i] = (byte) (i % 0x70);
    }


    static long memSize = 10 * 1024 * 1024 * 1024;
    static long cacheSize = memSize;
    static WriteBufferManager bufferManager = new WriteBufferManager(memSize,
            new LRUCache(cacheSize));

    public static void main(String[] args) throws RocksDBException {
        if (args.length < 3) {
            System.out.println("Param error : dataPath cfCount dataCount");
        }
        String dbPath = args[0];
        int cfCount = Integer.parseInt(args[1]);
        int dataCount = Integer.parseInt(args[2]);
        if (args.length > 3)
            memSize = Long.parseLong(args[3]) * 1024L * 1024L * 1024L;
        if (args.length > 4)
            cacheSize = Long.parseLong(args[4]) * 1024L * 1024L * 1024L;
        else
            cacheSize = memSize;
        writeDb(dbPath, cfCount, dataCount);
    }

    public static void readSST(String source) throws RocksDBException {

        try (Options options = new Options();
             ReadOptions readOptions = new ReadOptions()
                     .setIterStartSeqnum(1);
             SstFileReader reader = new SstFileReader(options)
        ) {
            reader.open(source);
            SstFileReaderIterator iterator = reader.newIterator(readOptions);

            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] iKey = iterator.key();
                byte[] uKey = Arrays.copyOfRange(iKey, 0, iKey.length - kNumInternalBytes);
                byte[] seq = Arrays.copyOfRange(iKey, iKey.length - kNumInternalBytes, iKey.length);
                System.out.println(new String(uKey) + " - " + toHex(seq));
                iterator.next();
            }

            TableProperties p = reader.getTableProperties();
            System.out.println(p);
        }
    }

    public static String toHex(byte[] bytes) {
        return new String(Hex.encodeHex(bytes));
    }


    public static void writeDb(String dbPath, int cfCount, int dataCount) throws RocksDBException {
        deleteDir(new File(dbPath));

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
            for (int i = 0; i < cfCount; i++) {
                cfDescriptors.add(new ColumnFamilyDescriptor(("cf" + i).getBytes(), cfOpts));
            }
            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            System.out.println("start open db");
            long start = System.currentTimeMillis();
            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true)
                    .setWriteBufferManager(bufferManager);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
                System.out.println("opened db " + (System.currentTimeMillis() - start));
                Thread[] threads = new Thread[cfCount];

                for (int cfIdx = 0; cfIdx < cfCount; cfIdx++) {
                    int finalCfIdx = cfIdx;
                    threads[cfIdx] = new Thread(() -> {
                        for (int i = 0; i < dataCount; i++) {
                            try {
                                db.put(columnFamilyHandles.get(finalCfIdx),
                                        String.format("cf%04d--%08d", finalCfIdx, i).getBytes(), value);
                            } catch (RocksDBException e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    threads[cfIdx].start();
                    System.out.println("Thread " + cfIdx);
                }
                System.out.println("等待结束");
                for (int cfIdx = 0; cfIdx < cfCount; cfIdx++) {
                    threads[cfIdx].join();
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    columnFamilyHandle.close();
                }

            } // frees the db and the db options
        }
    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }
}
