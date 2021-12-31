package org.example.rpc;

import com.alipay.sofa.jraft.rpc.RpcContext;
import com.alipay.sofa.jraft.rpc.RpcProcessor;
import com.alipay.sofa.jraft.rpc.RpcServer;
import lombok.Data;
import org.example.RaftEngine;

import java.io.Serializable;

public class SnapshotProcessor<T extends SnapshotProcessor.BaseRequest> implements RpcProcessor<T> {

    public static void registerProcessor(final RpcServer rpcServer, final RaftEngine engine){
        rpcServer.registerProcessor(new SnapshotProcessor<>(GetFileRequest.class, engine));
        rpcServer.registerProcessor(new SnapshotProcessor<>(TransFileRequest.class, engine));
    }

    private final Class<?> requestClass;
    private final RaftEngine engine;

    public SnapshotProcessor(Class<?> requestClass, RaftEngine engine) {
        this.requestClass = requestClass;
        this.engine = engine;
    }

    @Override
    public void handleRequest(RpcContext rpcCtx, T request) {
        switch (request.magic()) {
            case BaseRequest.GET_FILE:
                Status status = engine.getSnapshotFile((GetFileRequest) request);
                rpcCtx.sendResponse(new GetFileResponse() {{
                    setStatus(status);
                }});
                break;
            case BaseRequest.TRANS_FILE:
                status = engine.receiveSnapshotFile((TransFileRequest) request);
                rpcCtx.sendResponse(new TransFileResponse(){{ setStatus(status);}});
                break;
            default:
        }
    }

    @Override
    public String interest() {
        return this.requestClass.getName();
    }

    public abstract static class BaseRequest implements Serializable {
        public static final byte GET_FILE = 0x01;
        public static final byte TRANS_FILE = 0x02;

        public abstract byte magic();
    }

    @Data
    public abstract static class BaseResponse implements Serializable {
        private Status status;

    }

    /**
     * Follower发起的获取文件请求
     */
    @Data
    public static class GetFileRequest extends BaseRequest {
        private String graphName;
        private int partitionId;

        @Override
        public byte magic() {
            return GET_FILE;
        }
    }

    @Data
    public static class GetFileResponse extends BaseResponse {

    }

    /**
     * Leader发起的传输文件请求
     */
    @Data
    public static class TransFileRequest extends BaseRequest {
        private String graphName;
        private int partitionId;
        private String fileName;       // 文件名
        private byte[] data;           // 数据内容
        private int offset;          // 偏移量
        private int size;               // 数据大小
        private boolean eof;            // 该文件是否结束
        private boolean over;           // 所有文件是否结束
        private Status  status;         // 传输状态

        @Override
        public byte magic() {
            return TRANS_FILE;
        }
    }

    @Data
    public static class TransFileResponse extends BaseResponse {
    }

    public enum Status {
        OK(0, "OK"),
        NO_PARTITION_FOUND(1, "no partition"),
        IO_ERROR(2, "io error"),
        STOP(100, "");

        private final int code;
        private final String msg;

        Status(int code, String msg) {
            this.code = code;
            this.msg = msg;
        }
    }
}
