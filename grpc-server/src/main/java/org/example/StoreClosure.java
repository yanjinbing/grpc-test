package org.example;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Status;

public class StoreClosure implements Closure {

    private Closure      done;
    private Operation         operation;

    public StoreClosure(Operation op, Closure done){
        this.operation = op;
        this.done = done;
    }
    @Override
    public void run(Status status) {
        if ( done != null )
            done.run(status);
    }

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }
}
