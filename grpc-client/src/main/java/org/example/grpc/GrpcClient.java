package org.example.grpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.HashMap;
import java.util.Map;

public class GrpcClient {

    static String[][] PEERIDTOGRPC = {
            {"127.0.0.1:8081","127.0.0.1:8091"},
            {"127.0.0.1:8082","127.0.0.1:8092"},
            {"127.0.0.1:8083","127.0.0.1:8093"}
    };

    final String groupId = "a1";
    final String defaultAddr = "127.0.0.1:8091";
    String grpcAddr = defaultAddr;
    Map<String, String> peerIdToGrpc;

    public GrpcClient(){
        peerIdToGrpc = new HashMap<>();
        for(String[] peers : PEERIDTOGRPC){
            peerIdToGrpc.put(peers[0], peers[1]);
        }
    }

    public void flushLeaderAddr(){
        ManagedChannel channel = ManagedChannelBuilder.forTarget(defaultAddr).usePlaintext().build();
        //创建方法存根
        HelloWorldGrpc.HelloWorldBlockingStub stub = HelloWorldGrpc.newBlockingStub(channel);
        try{
            GetLeaderRequest request = GetLeaderRequest.newBuilder()
                    .setGroupId(groupId)
                    .build();
            GetLeaderReply reply = stub.getLeader(request);
            grpcAddr = peerIdToGrpc.get(reply.getLeader());
            System.out.println("Leader is " + reply.getLeader());
        }catch (Exception e){
            e.printStackTrace();
            e.getCause().printStackTrace();
        }
    }

    public void stressTest(int count){
        ManagedChannel channel = ManagedChannelBuilder.forTarget(grpcAddr).usePlaintext().build();
        //创建方法存根
        HelloWorldGrpc.HelloWorldBlockingStub stub = HelloWorldGrpc.newBlockingStub(channel);
        String groupId = "a1";
        for(int i = 0; i<count; i++) {
            // 构建消息
            HelloRequest request = HelloRequest.newBuilder()
                    .setGroupId(groupId)
                    .setKey("key " + System.currentTimeMillis())
                    .setValue("Value1")
                    .build();
            // 发送消息
            HelloReply response = stub.sayHello(request);
            System.out.println(response.getMessage());
        }
    }

    public static void main(String[] args){
        GrpcClient client = new GrpcClient();
        client.flushLeaderAddr();
        client.stressTest(100);


    }
}
