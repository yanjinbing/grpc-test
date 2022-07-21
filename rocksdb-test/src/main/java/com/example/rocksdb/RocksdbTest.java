package com.example.rocksdb;

import com.google.protobuf.ByteString;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.example.grpc.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RocksdbTest {
    public static void main(String args[]) {
        if ( args.length < 2)
            System.out.println("not found path");
        else
            testScan(args[0], args[1]);
    }

    public static void testScan(String dbPath, String cmd) {
        long totalCount = 0;
        int cmdValue = Integer.valueOf(cmd);
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()) {
            List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<ColumnFamilyDescriptor>();
            List<byte[]> columnFamilyBytes = RocksDB.listColumnFamilies(new Options(), dbPath);
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
                long totalStart = System.currentTimeMillis();
                for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                    try (ReadOptions readOptions = new ReadOptions()
                            .setIgnoreRangeDeletions(false);
                         RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions)) {
                        iterator.seekToFirst();
                        long count = 0;
                        long dcount = 0;
                        long start = System.currentTimeMillis();
                        while (iterator.isValid()) {
                            if ( cmdValue == 1){
                                byte[] key = iterator.key();
                                if ( key.length > 2)
                                    dcount += Arrays.copyOfRange(key, 0, key.length - 2).length;
                                else
                                    dcount += iterator.key().length;
                                dcount += iterator.value().length;
                            }else if ( cmdValue == 2){
                                byte[] key = iterator.key();
                                if ( key.length > 2)
                                    dcount += Arrays.copyOfRange(key, 0, key.length - 2).length;
                                else
                                    dcount += iterator.key().length;
                                dcount += iterator.value().length;
                                Kv kv = Kv.newBuilder().setKey(ByteString.copyFrom(key))
                                        .setValue(ByteString.copyFrom(iterator.value()))
                                        .build();
                                dcount += kv.getCode();
                            }else if ( cmdValue == 3){
                                byte[] key = iterator.key();
                                if ( key.length > 2)
                                    dcount += Arrays.copyOfRange(key, 0, key.length - 2).length;
                                else
                                    dcount += iterator.key().length;
                                dcount += iterator.value().length;
                                Kv kv = Kv.newBuilder().setKey(ByteString.copyFrom(key))
                                        .setValue(ByteString.copyFrom(iterator.value()))
                                        .build();
                                dcount += kv.toByteArray().length;
                            }else {
                                System.out.println("cmd error");
                                return;
                            }
                            iterator.next();
                            count++;
                            if ( count >= 10000000) {
                                System.out.println(" " + totalCount + "  qps is " + count * 1000/ (System.currentTimeMillis() - start) + " data = " + dcount);
                                totalCount += count;
                                count = 0;
                                start = System.currentTimeMillis();
                            }
                        }
                        totalCount += count;
                    }
                }
                System.out.println("Total is " + totalCount + " qps is " + totalCount *1000/ (System.currentTimeMillis() - totalStart));

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
}


