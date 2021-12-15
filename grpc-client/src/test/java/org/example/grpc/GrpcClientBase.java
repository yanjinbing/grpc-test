package org.example.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public class GrpcClientBase {

    public HelloWorldGrpc.HelloWorldBlockingStub getStub(String address) {
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        HelloWorldGrpc.HelloWorldBlockingStub stub = HelloWorldGrpc.newBlockingStub(channel);
        stub.withMaxInboundMessageSize(1024 * 1024 * 10);
        stub.withMaxOutboundMessageSize(1024 * 1024 * 10);
        return stub;
    }

    public HelloWorldGrpc.HelloWorldStub getStreamStub(String address){
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        HelloWorldGrpc.HelloWorldStub stub = HelloWorldGrpc.newStub(channel);
        return stub;
    }

    public void sendOne(String address, String groupId, ByteString key, ByteString value) {
        try {
            // 构建消息
            HelloRequest request = HelloRequest.newBuilder()
                    .setGroupId(groupId)
                    .setKey(key)
                    .setValue(value)
                    .build();
            // 发送消息
            HelloReply response = getStub(address).sayHello(request);
        } catch (Throwable e) {
            throw e;
        }

    }


    public void getLeader(String address, String groupId) {
        try {
            GetLeaderRequest request = GetLeaderRequest.newBuilder()
                    .setGroupId(groupId)
                    .build();
            HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);

            GetLeaderReply reply = stub.getLeader(request);
            System.out.println("Leader is " + reply.getLeader());
        } catch (Exception e) {
            e.printStackTrace();
            e.getCause().printStackTrace();
            throw e;
        }
    }

    /**
     * 向Leader添加Peer，要求raft node 已经启动
     */
    public void addPeer(String address, String groupId, String peer) {
        try {
            // 构建消息
            PeerRequest request = PeerRequest.newBuilder()
                    .setGroupId(groupId)
                    .setAddress(peer)
                    .build();
            HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);
            // 发送消息
            PeerReply response = stub.addPeer(request);
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }


    /**
     * Leader删除一个peer
     */
    public void removePeer(String address, String groupId, String peer) {
        try {
            // 构建消息
            PeerRequest request = PeerRequest.newBuilder()
                    .setGroupId(groupId)
                    .setAddress(peer)
                    .build();
            HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);
            // 发送消息
            PeerReply response = stub.removePeer(request);
        } catch (Throwable e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    /**
     * 启动raft node
     */
    public void startRaftNode(String address, String groupId, String peersList) {
        RaftNodeRequest request = RaftNodeRequest.newBuilder()
                .setGroupId(groupId)
                .setPeers(peersList)
                .build();
        HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);
        stub.startRaftNode(request);
    }

    public void stopRaftNode(String address, String groupId, String peersList) {
        RaftNodeRequest request = RaftNodeRequest.newBuilder()
                .setGroupId(groupId)
                .setPeers(peersList)
                .build();
        HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);
        stub.stopRaftNode(request);
    }

    /**
     * 设置工作模式，批量入库并且单副本的情况下关闭raft日志
     */
    public void setMode(String address, String groupId, int mode) {
        SetModeRequest request = SetModeRequest.newBuilder()
                .setGroupId(groupId)
                .setMode(mode)
                .build();
        getStub(address).setMode(request);
    }

    public void setBatchMode(String address, String groupId) {
        setMode(address, groupId, WorkMode.BATCH_LOADING_VALUE);
    }

    public void setNormalMode(String address, String groupId) {
        setMode(address, groupId, WorkMode.NORMAL_VALUE);
    }

    public void scan(String address, String id){
        ScanRequest request = ScanRequest.newBuilder().setId(id).build();
        StreamObserver<ScanRequest> requestStream = getStreamStub(address).scan(new StreamObserver<ScanResponse>() {
            @Override
            public void onNext(ScanResponse value) {
                System.out.println(value.getData().toStringUtf8());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {

            }
        });
        requestStream.onNext(request);
        requestStream.onCompleted();
    }
}
