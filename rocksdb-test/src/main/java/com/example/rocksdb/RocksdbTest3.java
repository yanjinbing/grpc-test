package com.example.rocksdb;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.DbPath;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class RocksdbTest3 {
    static {
        RocksDB.loadLibrary();
    }
    private static AtomicLong id;

    public static void main(String[] args) throws RocksDBException, InterruptedException, IOException {

            testPut("/data/test", 1000000);

      //  System.in.read();
        System.out.println("Start ...");
            testGet("/data/test");


    }

    private static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        return dir.delete();
    }

    public static void testGet(String dbPath) throws RocksDBException {

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();


            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {
                RocksIterator iterator = db.newIterator();
                long start = System.currentTimeMillis();
                for (int i = 0; i < 100; i++) {
                    iterator.seek("key=10".getBytes());
                }

                System.out.println("time is " + (System.currentTimeMillis() - start));
            } finally {
                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    columnFamilyHandle.close();
                }
            } // frees the db and the db options
        }
    }

    public static void testPut(String dbPath, int dataCount) throws RocksDBException, InterruptedException {
        new File(dbPath).mkdirs();
        deleteDir(new File(dbPath));


        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setMinWriteBufferNumberToMerge(20)
                .setMaxWriteBufferNumber(10)
                .setTargetFileSizeBase(1024 * 1024 * 1024)
                .setWriteBufferSize(64 * 1024 * 1024)
                .setLevel0FileNumCompactionTrigger(20)
                .setMaxBytesForLevelBase(1024 * 1024 * 1024)
                .setMaxBytesForLevelMultiplier(20)
                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            long count = 0;

            try (final DBOptions options = new DBOptions()

                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {


                    WriteBatch batch = new WriteBatch();
                    for (int i = 0; i < dataCount; i++) {
                        String key = String.format("key=%d", i);
                        batch.put(key.getBytes(), key.getBytes());
                        if (i % 1000 == 0) {
                            db.write(new WriteOptions(), batch);
                            batch.clear();
                            System.out.println(" " + (count++));
                        }
                    }
                db.write(new WriteOptions(), batch);
                    batch.clear();
                db.flush(new FlushOptions().setWaitForFlush(true));
                for (int j = 0; j < 100; j++) {
                    {
                        String key1 = String.format("key=%d", j);
                        String key2 = "key=999999999";
                        batch.deleteRange(key1.getBytes(), key2.getBytes());
                    }
                    db.write(new WriteOptions(), batch);
                }
                db.flush(new FlushOptions().setWaitForFlush(true));
            } finally {

                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    columnFamilyHandle.close();
                }

            } // frees the db and the db options
        }
    }
}
