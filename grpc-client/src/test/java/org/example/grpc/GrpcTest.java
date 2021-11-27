package org.example.grpc;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

public class GrpcTest {

    public HelloWorldGrpc.HelloWorldBlockingStub getStub(String address){
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        HelloWorldGrpc.HelloWorldBlockingStub stub = HelloWorldGrpc.newBlockingStub(channel);
        stub.withMaxInboundMessageSize(1024*1024*10);
        stub.withMaxOutboundMessageSize(1024*1024*10);
        return stub;
    }

    public void sendOne(String groupId, ByteString key, ByteString value){

        String address = "127.0.0.1:8091";

        try {
            // 构建消息
            HelloRequest request = HelloRequest.newBuilder()
                    .setGroupId(groupId)
                    .setKey(key)
                    .setValue(value)
                    .build();
            // 发送消息
            HelloReply response = getStub(address).sayHello(request);
        }catch (Throwable e){
            throw  e;
        }

    }


    public void getLeader(String groupId) {

        String address = "127.0.0.1:8091";
        ManagedChannel channel = ManagedChannelBuilder.forTarget(address).usePlaintext().build();
        //创建方法存根
        HelloWorldGrpc.HelloWorldBlockingStub stub = HelloWorldGrpc.newBlockingStub(channel);
        try {
            GetLeaderRequest request = GetLeaderRequest.newBuilder()
                    .setGroupId(groupId)
                    .build();
            try {
                GetLeaderReply reply = stub.getLeader(request);
                System.out.println("Leader is " + reply.getLeader());
            }catch (StatusRuntimeException e){
                e.printStackTrace();

            }
            channel.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            e.getCause().printStackTrace();
            throw e;
        }
    }

    public void addPeer(String address, String groupId,  String peer) {
        try {
            // 构建消息
            PeerRequest request = PeerRequest.newBuilder()
                    .setGroupId(groupId)
                    .setAddress(peer)
                    .build();
            HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);
            // 发送消息
            PeerReply response = stub.addPeer(request);
        }catch (Throwable e){
            System.out.println(e.getMessage());
            throw  e;
        }
    }


    public void removePeer(String address, String groupId,  String peer) {
        try {
            // 构建消息
            PeerRequest request = PeerRequest.newBuilder()
                    .setGroupId(groupId)
                    .setAddress(peer)
                    .build();
            HelloWorldGrpc.HelloWorldBlockingStub stub = getStub(address);
            // 发送消息
            PeerReply response = stub.removePeer(request);
        }catch (Throwable e){
            System.out.println(e.getMessage());
            throw  e;
        }
    }
    @Test
    public void testAddPeer(){
        String[] a1 = {"127.0.0.1:8091","127.0.0.1:8081"};
        String[] a2 = {"127.0.0.1:8092","127.0.0.1:8082"};
        String[] a3 = {"127.0.0.1:8093","127.0.0.1:8083"};
        String groupId = "a1";

        addPeer(a1[0],groupId, a2[1]);
        addPeer(a1[0],groupId, a3[1]);

    }

    @Test
    public void testRemovePeer(){
        String[] a1 = {"127.0.0.1:8091","127.0.0.1:8081"};
        String[] a2 = {"127.0.0.1:8092","127.0.0.1:8082"};
        String[] a3 = {"127.0.0.1:8093","127.0.0.1:8083"};
        String groupId = "a1";

        removePeer(a1[0],groupId, a2[1]);
        removePeer(a1[0],groupId, a3[1]);

    }

    @Test
    public void testSendOne(){
        getLeader("a1");
        sendOne("a1", ByteString.copyFromUtf8("Hello"), ByteString.copyFromUtf8("Word"));
    }

}
