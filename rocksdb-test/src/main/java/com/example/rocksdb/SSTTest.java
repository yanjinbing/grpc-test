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
public class SSTTest {
    static   int kNumInternalBytes = 8;      //internal key 增加的8个字节后缀
    private static byte[] value;

    static {
        value = new byte[1024];
        for (int i = 0; i < value.length; i++)
            value[i] = (byte) (i % 0x70);
    }

    public static void main(String[] args) throws RocksDBException {
        writeDb(args[0]);
        //String sstFile = "D:/test/rocksdbtest/000014.sst";
        //readSST(sstFile);
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

    public static void writeDb(String dbPath) throws RocksDBException {
        deleteDir(new File(dbPath));
        RocksDB.loadLibrary();
        List<AbstractEventListener> listeners = new ArrayList<>();
        listeners.add(new AbstractEventListener() {
            @Override
            public void onCompactionCompleted(RocksDB db, CompactionJobInfo compactionJobInfo) {
                super.onCompactionCompleted(db, compactionJobInfo);
                System.out.println("onCompactionCompleted " + db.getName());
            }
            @Override
            public void onCompactionBegin(final RocksDB db, final CompactionJobInfo compactionJobInfo) {
                System.out.println("onCompactionBegin " + db.getName());
            }
        });
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .setMinWriteBufferNumberToMerge(2)
                .setMaxWriteBufferNumber(4)
                .setTargetFileSizeBase(64 * 1024)
                .setWriteBufferSize(64 * 1024)
                .setLevel0FileNumCompactionTrigger(2)
                .setMaxBytesForLevelBase(128 * 1024)
                .setMaxBytesForLevelMultiplier(2)

                .setNumLevels(7)) {

            // list of column family descriptors, first entry must always be default column family
            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor("cf1".getBytes(), cfOpts)
            );

            // a list which will hold the handles for the column families once the db is opened
            final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

            try (final DBOptions options = new DBOptions()
                    .setCreateIfMissing(true)
                    .setPreserveDeletes(true)
                    .setListeners(listeners)
                    .setCreateMissingColumnFamilies(true)
                    .setWriteBufferManager(new WriteBufferManager(128*1024*1024,
                            new LRUCache(128*1024*1024)));
                 final RocksDB db = RocksDB.open(options, dbPath, cfDescriptors, columnFamilyHandles)) {

                try {
                    for (int i = 0; i < 30; i++) {
                        db.put(columnFamilyHandles.get(1), String.format("good%06d", i).getBytes(), value);
                    }

                   // db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles);

                  //  db.setPreserveDeletesSequenceNumber(db.getLatestSequenceNumber());
                    System.out.println(db.getLatestSequenceNumber());
                    long seqNo = 1;//db.getLatestSequenceNumber();

                    db.delete(columnFamilyHandles.get(1), String.format("good%06d", 9).getBytes());

                    db.deleteRange(columnFamilyHandles.get(1),
                            String.format("good%06d", 10).getBytes(), String.format("good%06d", 20).getBytes());
                    db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles);

                    for (int i = 15; i < 20; i++) {
                        db.put(columnFamilyHandles.get(1), String.format("good%06d", i).getBytes(), value);
                    }

                    db.flush(new FlushOptions().setWaitForFlush(true), columnFamilyHandles);
                 //   db.compactRange();
                    Thread.sleep(10000);
                    System.out.println("入库完成, last seqNo " + db.getLatestSequenceNumber());

                    for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        try (ReadOptions readOptions = new ReadOptions()
                                .setIterStartSeqnum(seqNo)
                                .setIgnoreRangeDeletions(false);
                             RocksIterator iterator = db.newIterator(columnFamilyHandle, readOptions)) {
                            iterator.seekToFirst();
                            int count = 0;
                            while (iterator.isValid()) {
                                if ( seqNo > 0 ) {
                                    byte[] iKey = iterator.key();
                                    byte[] uKey = Arrays.copyOfRange(iKey, 0, iKey.length - kNumInternalBytes);
                                    byte[] seq = Arrays.copyOfRange(iKey, iKey.length - kNumInternalBytes, iKey.length);
                                    System.out.println(new String(uKey) + " - " + toHex(seq));
                                }else
                                    System.out.println(new String(iterator.key()));
                                iterator.next();
                                count++;
                            }
                            System.out.println(count);
                        }
                    }

                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    // NOTE frees the column family handles before freeing the db
                    for (final ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
                        columnFamilyHandle.close();
                    }
                    db.close();
                } // frees the db and the db options
            }
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
