package org.example.grpc;

import com.google.protobuf.ByteString;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

public class GrpcTest extends GrpcClientBase{
    String[] a1 = {"127.0.0.1:8091","127.0.0.1:8081"};
    String[] a2 = {"127.0.0.1:8092","127.0.0.1:8082"};
    String[] a3 = {"127.0.0.1:8093","127.0.0.1:8083"};

    public String getLeader(){
        String groupId = "a1";
        String leader = getLeader(a2[0], groupId);
        System.out.println("Leader is " + leader);
        Map<String, String> peers = new HashMap<>();
        peers.put(a1[1], a1[0]);
        peers.put(a2[1], a2[0]);
        peers.put(a3[1], a3[0]);
        return peers.get(leader);
    }

    @Test
    public void transferLeader(){
        String groupId = "a1";
        String peer = a2[1];
        transferLeader(a3[0], groupId, peer);
    }
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
        String peerList = a1[1] + ";" + a2[1] + ";" + a3[1];
       // addPeer(a1[0],groupId, a2[1]);
       // addPeer(a1[0],groupId, a3[1]);
        changePeers(a1[0], groupId, peerList, "");
    }

    @Test
    public void testRemovePeer(){
        String groupId = "a1";
        //removePeer(a1[0],groupId, a2[1]);
        //removePeer(a1[0],groupId, a3[1]);
        String follower = a1[1];
        String learner = a2[1] + ";" +a3[1];
        // addPeer(a1[0],groupId, a2[1]);
        // addPeer(a1[0],groupId, a3[1]);
        changePeers(a1[0], groupId, follower, learner);

    }

    @Test
    public void testSendOne(){
        String groupId = "a1";
        //setNormalMode(a1[0], groupId);
        sendOne(a3[0],groupId,
                ByteString.copyFromUtf8("Hello"), ByteString.copyFromUtf8("Hello raft"));
    }

    @Test
    public void testBatchPut() throws InterruptedException {
        String groupId = "a1";

        String leader = getLeader();

        ExecutorService executor = new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(10000));
        for (long i = 0; i < 100000000000L; i++) {
            long finalI = i;
          //  executor.execute(() -> {
                sendOne(leader, groupId,
                        ByteString.copyFromUtf8("batch" + finalI), ByteString.copyFromUtf8("Hello raft"));

          //  });
            System.out.println(" " + i);
        }
        executor.shutdown();
        executor.awaitTermination(1000, TimeUnit.SECONDS);
    }

    @Test
    public void testScan(){
        scan(a1[0], "a1");

        try {
            Thread.sleep(100000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void test(){
        long l = System.currentTimeMillis();
        System.out.println(l);
        System.out.println(String.format("%016x", l));
    }
}
