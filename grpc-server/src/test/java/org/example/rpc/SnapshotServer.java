package org.example.rpc;

import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.entity.LocalFileMetaOutter;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.RaftOutter;
import com.alipay.sofa.jraft.error.RetryAgainException;
import com.alipay.sofa.jraft.option.RaftOptions;
import com.alipay.sofa.jraft.rpc.RaftRpcServerFactory;
import com.alipay.sofa.jraft.rpc.RpcServer;
import com.alipay.sofa.jraft.storage.FileService;
import com.alipay.sofa.jraft.storage.io.FileReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import com.alipay.sofa.jraft.storage.snapshot.local.LocalSnapshotMetaTable;
import com.alipay.sofa.jraft.storage.snapshot.local.SnapshotFileReader;
import com.alipay.sofa.jraft.util.ByteBufferCollector;
import com.alipay.sofa.jraft.util.Utils;
import org.junit.Test;

import java.io.IOException;
import java.util.Scanner;

public class SnapshotServer {
    public static final String REMOTE_SNAPSHOT_URI_SCHEME = "remote://";
    @Test
    public void testFileServer() throws IOException {
        String addr = "127.0.0.1:8500";
        String path = "D:/test/9";
        PeerId serverId = JRaftUtils.getPeerId(addr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint(),
                JRaftUtils.createExecutor("RAFT-RPC-", Utils.cpus() * 6),null);

        rpcServer.init(null);


        LocalSnapshotMetaTable metaTable = new LocalSnapshotMetaTable(new RaftOptions());
        // 添加文件
        metaTable.addFile("file.txt", LocalFileMetaOutter.LocalFileMeta.newBuilder().build());
        // 保存meta
        metaTable.saveToFile(path + "meta.txt");

        // 创建文件访问器
        final SnapshotFileReader reader = new SnapshotFileReader(path, null);
        reader.setMetaTable(metaTable);

        // 添加到文件服务
        long readerId = FileService.getInstance().addReader(reader);

        System.out.println(String.format(REMOTE_SNAPSHOT_URI_SCHEME + "%s/%d", addr, readerId));

        // 等待结束
        Scanner input = new Scanner(System.in);
        input.next();

        FileService.getInstance().removeReader(readerId);
        rpcServer.shutdown();
    }

    @Test
    public void testSnapshotServer() throws IOException {
        String addr = "127.0.0.1:8500";
        String path = "D:/test/9";

        PeerId serverId = JRaftUtils.getPeerId(addr);
        RpcServer rpcServer = RaftRpcServerFactory.createRaftRpcServer(serverId.getEndpoint(),
                JRaftUtils.createExecutor("RAFT-RPC-", Utils.cpus() * 6),null);

        rpcServer.init(null);

        final RaftOutter.SnapshotMeta.Builder metaBuilder = RaftOutter.SnapshotMeta.newBuilder() //
                .setLastIncludedIndex(10) //
                .setLastIncludedTerm(1);

        SnapshotStorageImpl storage = new SnapshotStorageImpl(path, new RaftOptions());
        storage.init(null);
        storage.setServerAddr(JRaftUtils.getEndPoint(addr));
        SnapshotWriter writer = storage.create();
        writer.saveMeta(metaBuilder.build());
        writer.addFile("file.txt");

        writer.close();

        SnapshotReader reader = storage.open();
        reader.load();
        String uri = reader.generateURIForCopy();

        System.out.println(uri);

        // 等待结束
        Scanner input = new Scanner(System.in);
        input.next();

        rpcServer.shutdown();
    }


}
