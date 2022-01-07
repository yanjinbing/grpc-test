package org.example.rpc;

import com.alipay.sofa.jraft.option.RpcOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SnapshotRpcClientTest {

    @Test
    public void testGetSnapshot() throws ExecutionException, InterruptedException {
        SnapshotRpcClient client = new SnapshotRpcClient();
        client.init(new RpcOptions());
        SnapshotProcessor.GetSnapshotRequest request = new SnapshotProcessor.GetSnapshotRequest();
        request.setGraphName("test");
        request.setSeqNum(2);
        request.setPartitionId(1);
        SnapshotProcessor.GetSnapshotResponse response
                =  client.getSnapshot("127.0.0.1:8081", request).get();
        System.out.println(response);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        for(int i = 0; i<10000; i++) {
            SnapshotRpcClient client = new SnapshotRpcClient();
            client.init(new RpcOptions());
            SnapshotProcessor.GetSnapshotRequest request = new SnapshotProcessor.GetSnapshotRequest();
            request.setGraphName("test");
            request.setSeqNum(i);
            request.setPartitionId(1);
            SnapshotProcessor.GetSnapshotResponse response
                    =  client.getSnapshot("127.0.0.1:8081", request).get();
        }
    }

    @Test
    public void testTransSnapshot() throws ExecutionException, InterruptedException {
        SnapshotRpcClient client = new SnapshotRpcClient();
        client.init(new RpcOptions());
        List<byte[]> datas = new ArrayList<>();
        datas.add("Hello".getBytes());
        datas.add("World".getBytes());
        SnapshotProcessor.TransSnapshotRequest request = new SnapshotProcessor.TransSnapshotRequest();
        request.setGraphName("graph");
        request.setPartitionId(1);
        request.setStartSeqNum(1);
        request.setEndSeqNum(1);
        request.setStatus(SnapshotProcessor.Status.INCOMPLETE);
        request.setData(datas);

        SnapshotProcessor.TransSnapshotResponse response
                =  client.transSnapshot("127.0.0.1:8081", request).get();
        System.out.println(response);
    }
}
