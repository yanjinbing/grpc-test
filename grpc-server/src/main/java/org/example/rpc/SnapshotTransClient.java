package org.example.rpc;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.option.RpcOptions;
import com.alipay.sofa.jraft.rpc.InvokeCallback;
import com.alipay.sofa.jraft.rpc.InvokeContext;
import com.alipay.sofa.jraft.rpc.RaftRpcFactory;
import com.alipay.sofa.jraft.rpc.RpcClient;
import com.alipay.sofa.jraft.rpc.impl.BoltRpcClient;
import com.alipay.sofa.jraft.util.Endpoint;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;

import java.util.concurrent.Executor;

public class SnapshotTransClient {
    protected volatile RpcClient rpcClient;
    private RpcOptions rpcOptions;

    public synchronized boolean init(final RpcOptions rpcOptions) {
        this.rpcOptions = rpcOptions;
        final RaftRpcFactory factory = RpcFactoryHelper.rpcFactory();
        this.rpcClient = factory.createRpcClient(factory.defaultJRaftClientConfigHelper(this.rpcOptions));
        return this.rpcClient.init(null);

    }

    private <V> void internalCallAsyncWithRpc(final Endpoint endpoint, final SnapshotProcessor.BaseRequest request,
                                              final ClosureAdapter<V> closure) {
        final InvokeContext invokeCtx = null;
        final InvokeCallback invokeCallback = new InvokeCallback() {

            @Override
            public void complete(final Object result, final Throwable err) {
                if (err == null) {
                    final SnapshotProcessor.BaseResponse response = (SnapshotProcessor.BaseResponse) result;
                    closure.setResponse((V) response);
                } else {
                    closure.run(new Status(-1, err.getMessage()));
                }
            }
        };

        try {
            this.rpcClient.invokeAsync(endpoint, request, invokeCtx, invokeCallback, this.rpcOptions.getRpcDefaultTimeout());
        } catch (final Throwable t) {
            closure.run(new Status(-1, t.getMessage()));
        }
    }

    public static abstract class ClosureAdapter<T> implements Closure {
        private T resp;

        public T getResponse() {
            return this.resp;
        }

        public void setResponse(T resp) {
            this.resp = resp;
        }
    }

}
