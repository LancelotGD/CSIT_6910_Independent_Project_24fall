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

public class Q5RegionProcessFunction extends KeyedProcessFunction<Object, Payload, Payload> {
    String nextKey = "R_REGIONKEY";
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        TypeInformation<Set<Payload>> ti = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> vsd = new ValueStateDescriptor<>("Q5RegionAlive", ti);
        aliveSet = getRuntimeContext().getState(vsd);
    }

    public boolean isValid(Payload p) {
        String r_name = (String)p.getValueByColumnName("R_NAME");
        return "ASIA".equals(r_name);
    }

    @Override
    public void processElement(Payload value, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
        }

        Payload tmp = new Payload(value); tmp.type = "Tmp";

        if(isValid(value)){
            if("Insert".equals(value.type)){
                if(aliveSet.value().add(tmp)){
                    value.type = "SetLive";
                    value.setKey(nextKey);
                    out.collect(value);
                }
            } else {
                if(aliveSet.value().remove(tmp)){
                    value.type = "SetDead";
                    value.setKey(nextKey);
                    out.collect(value);
                }
            }
        }
    }
}
