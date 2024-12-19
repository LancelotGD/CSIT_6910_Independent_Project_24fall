package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Q10NationProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    String nextKey = "NATIONKEY";
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Payload>> typeInformation = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> aliveDescriptor =
                new ValueStateDescriptor<>("Q10NationProcessFunctionAlive", typeInformation);
        aliveSet = getRuntimeContext().getState(aliveDescriptor);
    }

    public boolean isValid(Payload payload) {
        return true;
    }

    @Override
    public void processElement(Payload value, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }

        if(isValid(value)) {
            Payload tmp = new Payload(value);
            tmp.type = "Tmp";

            Set<Payload> set = aliveSet.value();
            if("Insert".equals(value.type)){
                if(set.add(tmp)){
                    value.type = "SetLive";
                    value.setKey(nextKey);
                    out.collect(value);
                }
            } else {
                if(set.remove(tmp)){
                    value.type = "SetDead";
                    value.setKey(nextKey);
                    out.collect(value);
                }
            }
        }
    }
}
