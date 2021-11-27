package org.example;

import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.option.SnapshotCopierOptions;
import com.alipay.sofa.jraft.storage.SnapshotThrottle;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotCopier;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotStorage;
import com.alipay.sofa.jraft.util.Endpoint;
import com.google.protobuf.Message;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;

public class SnapshotStorageImpl extends LocalSnapshotStorage {
    RaftOutter.SnapshotMeta snapshotMeta = null;
    public SnapshotStorageImpl(final String uri, final RaftOptions raftOptions){
        super(uri, raftOptions);

    }
    @Override
    public boolean setFilterBeforeCopyRemote() {
        System.out.println("SnapshotStorage setFilterBeforeCopyRemote");
        return super.setFilterBeforeCopyRemote();
    }

    @Override
    public SnapshotWriter create() {
        System.out.println("SnapshotStorage create");
        return super.create();

    }

    @Override
    public SnapshotReader open() {
        System.out.println("SnapshotStorage open");
        SnapshotWriter writer = create();
        // 延后生成快照文件
        String filePath = writer.getPath() + File.separator + "snapshot2";
        try {
            FileUtils.writeStringToFile(new File(filePath), "snapshot2", Charset.defaultCharset());
            writer.addFile("snapshot2");

            writer.close(true);
            writer.shutdown();
        }catch (Exception e){e.printStackTrace();}


        SnapshotReader reader = super.open();
        return reader != null ?
                new SnapshotReaderImpl(null, null, null, null, null)
                        .setReader(reader)
                        .setWriter(create()) : null;
    }

    @Override
    public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
        System.out.println("SnapshotStorage copyFrom");
        return super.copyFrom(uri, opts);
    }

    @Override
    public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
        System.out.println("SnapshotStorage startToCopyFrom");
        return super.startToCopyFrom(uri, opts);
    }

    @Override
    public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
        System.out.println("SnapshotStorage setSnapshotThrottle");
        super.setSnapshotThrottle(snapshotThrottle);
    }

    @Override
    public boolean init(Void opts) {
        System.out.println("SnapshotStorage init");
        return super.init(opts);
    }

    @Override
    public void shutdown() {
        super.shutdown();
    }


    //=========================================================================//
    // SnapshotReader代理类，LocalSnapshotCopier使用的是LocalSnapshotReader
    static class SnapshotReaderImpl extends LocalSnapshotReader {

        private SnapshotReader reader;
        private SnapshotWriter writer;
        public SnapshotReaderImpl(LocalSnapshotStorage snapshotStorage, SnapshotThrottle snapshotThrottle, Endpoint addr, RaftOptions raftOptions, String path) {
            super(snapshotStorage, snapshotThrottle, addr, raftOptions, path);
        }

        public SnapshotReaderImpl setReader(SnapshotReader reader) {
            this.reader = reader;
            return this;
        }

        public SnapshotReaderImpl setWriter(SnapshotWriter writer){
            this.writer = writer;
            return this;
        }

        @Override
        public RaftOutter.SnapshotMeta load() {
            System.out.println("SnapshotReader load");
            return reader.load();
        }

        @Override
        public String generateURIForCopy() {

            String uri =  reader.generateURIForCopy();
            System.out.println("SnapshotReader generateURIForCopy " + uri);
            return uri;
        }

        @Override
        public boolean init(Void opts) {
            System.out.println("SnapshotReader init " + opts);
            return reader.init(opts);
        }

        @Override
        public void shutdown() {
            reader.shutdown();
        }

        @Override
        public String getPath() {
            String path = reader.getPath();
            System.out.println("SnapshotReader getPath " + path);
            return path;
        }

        @Override
        public Set<String> listFiles() {
            System.out.println("SnapshotReader listFiles");
            return reader.listFiles();
        }

        @Override
        public Message getFileMeta(String fileName) {
            System.out.println("SnapshotReader getFileMeta " + fileName);
            return reader.getFileMeta(fileName);
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    static class SnapshotCopierImpl extends SnapshotCopier {

        @Override
        public void cancel() {

        }

        @Override
        public void join() throws InterruptedException {

        }

        @Override
        public void start() {

        }

        @Override
        public SnapshotReader getReader() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    static class SnapshotWriterImpl extends  SnapshotWriter{
        SnapshotStorageImpl storage;
        public SnapshotWriterImpl(SnapshotStorageImpl storage){
            this.storage = storage;
        }
        @Override
        public boolean saveMeta(RaftOutter.SnapshotMeta meta) {
            System.out.println("SnapshotWriter saveMeta " + meta);
            storage.snapshotMeta = meta;
            return true;
        }

        @Override
        public boolean addFile(String fileName, Message fileMeta) {
            System.out.println("SnapshotWriter addFile " + fileName + " " + fileMeta);
            return true;
        }

        @Override
        public boolean removeFile(String fileName) {
            return false;
        }

        @Override
        public void close(boolean keepDataOnError) throws IOException {

        }

        @Override
        public boolean init(Void opts) {
            System.out.println("Snapshot init " + opts);
            return true;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public String getPath() {
            System.out.println("SnapshotWriter getPath");
            return null;
        }

        @Override
        public Set<String> listFiles() {
            System.out.println("SnapshotWriter listFiles");
            return null;
        }

        @Override
        public Message getFileMeta(String fileName) {
            System.out.println("SnapshotWriter getFileMeta " + fileName);
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }
}
