package com.example.rocksdb;

import com.sun.tools.corba.se.idl.constExpr.Or;
import net.openhft.chronicle.map.ChronicleMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    long counter = 0;

    void add(){
        counter++;
    }
    private enum OrderState {
        NEW(0),
        WORKING(1),
        COMPLETED(10);

        int value;

        OrderState(int value) {
            this.value = value;
        }
    }

    OrderState state = OrderState.NEW;

    public static void main(String[] args) throws Exception {

        Matcher matcher = Pattern.compile("(^#|^//).*|")
                .matcher("Constants.EMPTY_STR");
        matcher.matches();
    }
}
