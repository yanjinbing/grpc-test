package org.example;


import com.alipay.sofa.jraft.*;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.example.grpc.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Hello world!
 */
public class GrpcServer {
    private Server grpcServer;
    // raft状态机
    private RaftParams raftParams;
    private Map<String, RaftGroup> raftGroups = new HashMap<>();

    private void start(int port) throws IOException {
        // 启动grpc服务
        grpcServer = ServerBuilder.forPort(port)
                .addService(new HelloWorldImpl(this))
                .build()
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                GrpcServer.this.stop();
            }
        });
    }

    private void stop() {
        if (grpcServer != null) {
            grpcServer.shutdown();
        }
    }

    public Node getRaftNode(String groupId) {
        if (!raftGroups.containsKey(groupId))
            startRaft(groupId, raftParams.dataPath, raftParams.raftAddr, raftParams.peersList);
        return raftGroups.get(groupId).getRaftNode();
    }

    /**
     * 启动raft服务
     *
     * @param dataPath  存储路径
     * @param address   服务地址
     * @param peersList 集群地址
     */
    private void startRaft(String groupId, String dataPath, String address, String peersList) {
        PeerId serverId = JRaftUtils.getPeerId(address);

        // 创建状态机
        MyStateMachine stateMachine = new MyStateMachine();
        Configuration initConf = new Configuration();
        initConf.parse(peersList);
        // 设置Node参数，包括日志存储路径和状态机实例
        NodeOptions nodeOptions = new NodeOptions();
        nodeOptions.setFsm(stateMachine);
        // 日志路径
        nodeOptions.setLogUri(dataPath + "/log");
        nodeOptions.setRaftMetaUri(dataPath + "/meta");
        nodeOptions.setInitialConf(initConf);
        // 构建raft组并启动raft
        RaftGroupService cluster = new RaftGroupService(groupId, serverId, nodeOptions);
        Node raftNode = cluster.start();
        raftGroups.put(groupId, new RaftGroup(stateMachine, raftNode, cluster));
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    System.out.printf("shutdownHook");
                    raftNode.shutdown();
                }));

    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    public PeerId getLeader(String groupId){
        return  getRaftNode(groupId).getLeaderId();
    }

    /**
     * 实现grpc服务
     */
    static class HelloWorldImpl extends HelloWorldGrpc.HelloWorldImplBase {
        private GrpcServer server;

        public HelloWorldImpl(GrpcServer server) {
            this.server = server;
        }

        //实现grpc方法
        public void sayHello(HelloRequest request, StreamObserver<HelloReply> observer) {
            String key = request.getKey();
            String value = request.getValue();
            System.out.println("收到数据 " + key);
            //创建任务，发送给其他peer

           String groupId = request.getGroupId();
            try {
                // 提交raft 任务
                putTask(groupId, request.getKeyBytes().toByteArray(), value.getBytes(StandardCharsets.UTF_8),
                        new Closure() {
                            @Override
                            public void run(Status status) {
                                System.out.println("收到应答" + status);
                                observer.onNext(HelloReply.newBuilder().setMessage("response " + request.getKey()).build());
                                observer.onCompleted();
                            }
                        });
                System.out.println("提交raft任务");
            } catch (IOException e) {
                observer.onError(e);
                observer.onCompleted();
            }
        }

        public void getLeader(GetLeaderRequest request, StreamObserver<GetLeaderReply> observer) {

            String groupId = request.getGroupId();
            PeerId peerId = server.getLeader(groupId);
            if ( peerId != null ){
                GetLeaderReply reply = GetLeaderReply.newBuilder()
                        .setLeader(peerId.toString())
                        .setGroupId(groupId)
                        .build();
                observer.onNext(reply);
            }else
                observer.onError(io.grpc.Status.fromCodeValue(1).asException());
            observer.onCompleted();
        }

        protected void putTask(final String groupId, final byte[] key, final byte[] value, Closure done) throws IOException {
            Operation op = Operation.createPut(key, value);
            // 序列化
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(bos);
            os.writeObject(op);
            os.close();

            // 创建并提交任务
            final Task task = new Task();
            task.setData(ByteBuffer.wrap(bos.toByteArray()));
            task.setDone(new StoreClosure(op, done));
            this.server.getRaftNode(groupId).apply(task);
        }
    }

    class RaftGroup{
        private MyStateMachine stateMachine;
        private Node raftNode;
        private RaftGroupService service;
        public RaftGroup(MyStateMachine machine, Node node, RaftGroupService service){
            this.stateMachine = machine;
            this.service = service;
            this.raftNode = node;
        }
        public boolean isLeader() {
            return stateMachine.isLeader();
        }

        public Node getRaftNode(){
            return raftNode;
        }

    }
    static class RaftParams {
        public String dataPath;
        public String raftAddr;
        public String peersList;
        public RaftParams(String dataPath, String raftAddr, String peersList){
            this.dataPath = dataPath;
            this.raftAddr = raftAddr;
            this.peersList = peersList;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {

        if (args.length != 4) {
            System.out.println("Useage : {dataPath} {grpcPort} {raftAddr} {peersList}");
            System.out.println("Example:  /tmp/server1 127.0.0.1:8081 127.0.0.1:8081,127.0.0.1:8082,127.0.0.1:8083");
            System.exit(1);
        }

        final String dataPath = args[0];
        final String grpcPort = args[1];
        final String raftAddr = args[2];
        final String peersList = args[3];

        System.out.println("Start grpc server raft addr is " + raftAddr + ", grpc port is " + grpcPort);
        final GrpcServer server = new GrpcServer();
        server.raftParams = new RaftParams(dataPath, raftAddr, peersList);
        new File(dataPath).mkdirs();
        server.start(Integer.valueOf(grpcPort));
        server.getRaftNode("a1");
        server.blockUntilShutdown();
    }
}
