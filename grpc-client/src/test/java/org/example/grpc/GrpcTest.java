package org.example.grpc;

import com.google.protobuf.ByteString;
import org.junit.Test;

public class GrpcTest extends GrpcClientBase{
    String[] a1 = {"127.0.0.1:8091","127.0.0.1:8081"};
    String[] a2 = {"127.0.0.1:8092","127.0.0.1:8082"};
    String[] a3 = {"127.0.0.1:8093","127.0.0.1:8083"};

    @Test
    public void startRaftNode(){
        String groupId = "a1";
        startRaftNode(a2[0], groupId, a2[1]);
        startRaftNode(a3[0], groupId, a3[1]);
    }

    @Test
    public void stopRaftNode(){
        String groupId = "a1";
        stopRaftNode(a2[0], groupId, "");
        stopRaftNode(a3[0], groupId, "");
    }

    @Test
    public void testAddPeer(){
        String groupId = "a1";
        addPeer(a1[0],groupId, a2[1]);
        addPeer(a1[0],groupId, a3[1]);

    }

    @Test
    public void testRemovePeer(){
        String groupId = "a1";
        removePeer(a1[0],groupId, a2[1]);
        removePeer(a1[0],groupId, a3[1]);

    }

    @Test
    public void testSendOne(){
        String groupId = "a1";
        getLeader(a1[0], groupId);
        sendOne(a1[0],groupId,
                ByteString.copyFromUtf8("Hello"), ByteString.copyFromUtf8("Word"));
    }

}
