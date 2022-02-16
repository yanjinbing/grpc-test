package org.example.rpc;

import com.alipay.sofa.jraft.option.RpcOptions;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class SnapshotRpcClientTest {

    @Test
    public void testGetSnapshot() throws ExecutionException, InterruptedException {
        CmdClient client = new CmdClient();
        client.init(new RpcOptions());
        CmdProcessor.GetSnapshotRequest request = new CmdProcessor.GetSnapshotRequest();
        request.setGraphName("test");
        request.setSeqNum(2);
        request.setPartitionId(1);
        CmdProcessor.GetSnapshotResponse response
                =  client.getSnapshot("127.0.0.1:8081", request).get();
        System.out.println(response);
    }

    @Test
    public void test() throws ExecutionException, InterruptedException {
        for(int i = 0; i<10000; i++) {
            CmdClient client = new CmdClient();
            client.init(new RpcOptions());
            CmdProcessor.GetSnapshotRequest request = new CmdProcessor.GetSnapshotRequest();
            request.setGraphName("test");
            request.setSeqNum(i);
            request.setPartitionId(1);
            CmdProcessor.GetSnapshotResponse response
                    =  client.getSnapshot("127.0.0.1:8081", request).get();
        }
    }

    @Test
    public void testTransSnapshot() throws ExecutionException, InterruptedException {
        CmdClient client = new CmdClient();
        client.init(new RpcOptions());
        List<byte[]> datas = new ArrayList<>();
        datas.add("Hello".getBytes());
        datas.add("World".getBytes());
        CmdProcessor.TransSnapshotRequest request = new CmdProcessor.TransSnapshotRequest();
        request.setGraphName("graph");
        request.setPartitionId(1);
        request.setStartSeqNum(1);
        request.setEndSeqNum(1);
        request.setStatus(CmdProcessor.Status.INCOMPLETE);
        request.setData(datas);

        CmdProcessor.TransSnapshotResponse response
                =  client.transSnapshot("127.0.0.1:8081", request).get();
        System.out.println(response);
    }
}
