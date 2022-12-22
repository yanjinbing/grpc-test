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
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RocksdbTest2 {
    static {
        RocksDB.loadLibrary();
    }
    private static AtomicLong id;
    public static void main(String[] args) throws RocksDBException, InterruptedException {

      //  testPut("/data/test", 1000000);
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
        final List<DbPath> dbPaths = new ArrayList<>();
        dbPaths.add(new DbPath(Paths.get("/data", "a"), 1*1024*1024*1024));
        dbPaths.add(new DbPath(Paths.get("/data","c"), 8*1024*1024*1024));
        dbPaths.add(new DbPath(Paths.get("/data","b"), 8*1024*1024*1024));

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            long count = 0;

            try (final DBOptions options = new DBOptions()
                    .setDbPaths(dbPaths)
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {


                RocksIterator iterator = db.newIterator();
                iterator.seekToFirst();
                while (iterator.isValid()){
                    iterator.next();
                    count++;
                }
                System.out.println("count is " + count);
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

        final List<DbPath> dbPaths = new ArrayList<>();
        dbPaths.add(new DbPath(Paths.get("/data", "a"), 1*1024*1024*1024));
        dbPaths.add(new DbPath(Paths.get("/data","c"), 8*1024*1024*1024));
        dbPaths.add(new DbPath(Paths.get("/data","b"), 8*1024*1024*1024));

        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
            long count = 0;

            try (final DBOptions options = new DBOptions()
                    .setDbPaths(dbPaths)
                    .setCreateIfMissing(true)
                    .setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {

                WriteBatch batch = new WriteBatch();
                for (int i = 0; i < dataCount; i++) {
                    String key = String.format("Rocksdb key = %d", i);
                   // String key = getMd5("" + i ) + getMd5("a" + i) + getId();
                  //  String value = "10000" + getId() + getId();
                    batch.put(key.getBytes(), value);
                    if ( i % 1000 == 0){
                        db.write(new WriteOptions(), batch);
                        batch.clear();
                        System.out.println(" " + (count++));

                    }
                }
                db.write(new WriteOptions(), batch);
                db.flush(new FlushOptions().setWaitForFlush(true));
            } finally {

                // NOTE frees the column family handles before freeing the db
                for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    columnFamilyHandle.close();
                }
            } // frees the db and the db options
        }
    }
    private static byte[] value;

    static {
        value = new byte[1024];
        for (int i = 0; i < value.length; i++)
            value[i] = (byte) (i % 0x70);
    }
    public static String getMd5(String txt){
        String rs = "";
        String[] hexDigits = { "0", "1", "2", "3", "4", "5", "6", "7", "8","9", "a", "b", "c", "d", "e", "f" };
        try {
            MessageDigest messageDigest =  MessageDigest.getInstance("MD5");
            byte[] b = messageDigest.digest(txt.getBytes());
            StringBuffer resultSb = new StringBuffer();
            for (int i = 0; i < b.length; i++) {
                int n = b[i];
                if (n < 0)
                    n = 256 + n;
                int d1 = n / 16;
                int d2 = n % 16;
                resultSb.append(hexDigits[d1] + hexDigits[d2]);
            }
            rs = resultSb.toString();
        }catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return rs;
    }

    public synchronized static Long getId() {
        //如果需要更长 或者更大冗余空间, 只需要 time * 10^n   即可
        //当前可保证1毫秒 生成 10000条不重复
        Long time = Long.valueOf(new SimpleDateFormat("HHmmssSSS").format(new Date())) * 10000 + (long) (Math.random() * 100);
//                    Long.valueOf(new SimpleDateFormat("yyMMddhhmmssSSS").format(new Date()).toString()) * 10000;
        if (id == null) {
            id = new AtomicLong(time);
            return id.get();
        }
        if (time <= id.get()) {
            id.addAndGet(1);
        } else {
            id = new AtomicLong(time);
        }
        return id.get();
    }
}
