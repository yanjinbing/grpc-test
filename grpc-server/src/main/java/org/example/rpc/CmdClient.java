package org.example.rpc;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

import java.util.concurrent.CompletableFuture;

public class CmdClient {
    protected volatile RpcClient rpcClient;
    private RpcOptions rpcOptions;

    public synchronized boolean init(final RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
        final RaftRpcFactory factory = RpcFactoryHelper.rpcFactory();
        this.rpcClient = factory.createRpcClient(factory.defaultJRaftClientConfigHelper(this.rpcOptions));
        return this.rpcClient.init(null);
    }

    /**
     * 请求快照
     */
    public CompletableFuture<CmdProcessor.GetSnapshotResponse>
    getSnapshot(final String address, CmdProcessor.GetSnapshotRequest request ) {
        ClosureAdapter<CmdProcessor.GetSnapshotResponse> response = new ClosureAdapter<>();
        internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, response);
        return response.future;
    }

    /**
     * 传输快照
     */
    public CompletableFuture<CmdProcessor.TransSnapshotResponse>
    transSnapshot(final String address, CmdProcessor.TransSnapshotRequest request) {
        ClosureAdapter<CmdProcessor.TransSnapshotResponse> response = new ClosureAdapter<>();
        internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, response);
        return response.future;
    }

    /**
     * 传输快照
     */
    public CompletableFuture<CmdProcessor.InstallSnapshotResponse>
    installSnapshot(final String address, CmdProcessor.InstallSnapshotRequest request) {
        ClosureAdapter<CmdProcessor.InstallSnapshotResponse> response = new ClosureAdapter<>();
        internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, response);
        return response.future;
    }

    /**
     * 传输快照
     */
    public CompletableFuture<CmdProcessor.InstallSnapshotOKResponse>
    installSnapshotOK(final String address, CmdProcessor.InstallSnapshotOKRequest request) {
        ClosureAdapter<CmdProcessor.InstallSnapshotOKResponse> response = new ClosureAdapter<>();
        internalCallAsyncWithRpc(JRaftUtils.getEndPoint(address), request, response);
        return response.future;
    }



    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint, final CmdProcessor.BaseRequest request,
                                  final ClosureAdapter<V> closure) {
        final InvokeContext invokeCtx = null;
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final CmdProcessor.BaseResponse response = (CmdProcessor.BaseResponse) result;
                    closure.setResponse((V) response);
                } else {
                    closure.failure(err);
                    closure.run(new Status(-1, err.getMessage()));
                }
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback, this.rpcOptions.getRpcDefaultTimeout());
        } catch (final Throwable t) {
            closure.failure(t);
            closure.run(new Status(-1, t.getMessage()));
        }
    }

    public static class ClosureAdapter<T> implements Closure {
        private final CompletableFuture<T> future = new CompletableFuture<>();
        private T resp;

        public T getResponse() { return this.resp; }

        public void setResponse(T resp) {
            this.resp = resp;
            future.complete(resp);
        }

        public void failure(Throwable t){
            future.completeExceptionally(t);
        }

        @Override
        public void run(Status status) {

        }
    }

}
