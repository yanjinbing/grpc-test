package org.example;

import com.alipay.sofa.jraft.util.Requires;

import java.io.Serializable;

public class Operation implements Serializable {


    private byte[]              key;                                    // also startKey for DELETE_RANGE
    private byte[]              value;
    private byte                op;

    /** Encode magic number */
    public static final byte    MAGIC            = 0x00;

    /** Put operation */
    public static final byte    PUT              = 0x01;
    public Operation(byte[] key, byte[] value, Object attach, byte op) {
        this.key = key;
        this.value = value;
        this.op = op;
    }
    public static Operation createPut(final byte[] key, final byte[] value) {
        Requires.requireNonNull(key, "key");
        Requires.requireNonNull(value, "value");
        return new Operation(key, value, null, PUT);
    }

    public byte[] getKey() {
        return key;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public byte getOp() {
        return op;
    }

    public void setOp(byte op) {
        this.op = op;
    }
}
