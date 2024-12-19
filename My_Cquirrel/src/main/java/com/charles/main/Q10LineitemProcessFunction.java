package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q10LineitemProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    String nextKey = "CUSTKEY";

    ValueState<Integer> aliveCount;
    ValueState<Payload> alivePayload;
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("Q10LineitemProcessFunctionCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Payload> aliveNodeDesc = new ValueStateDescriptor<>("Q10LineitemProcessFunctionAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(aliveNodeDesc);

        TypeInformation<Set<Payload>> ti = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q10LineitemProcessFunctionAliveSet", ti);
        aliveSet = getRuntimeContext().getState(setDesc);
    }

    public boolean isValid(Payload value) throws ParseException {
        String returnFlag = (String)value.getValueByColumnName("L_RETURNFLAG");
        return "R".equals(returnFlag);
    }

    @Override
    public void processElement1(Payload payload, Context ctx, Collector<Payload> collector) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if("SetLive".equals(payload.type)){
            alivePayload.update(new Payload(payload));
            aliveCount.update(aliveCount.value() + 1);
            for(Payload p : aliveSet.value()){
                payload.type = "Add";
                outputPayload(payload, p, collector);
            }
        }else{
            aliveCount.update(aliveCount.value() - 1);
            for(Payload p : aliveSet.value()){
                payload.type = "Sub";
                outputPayload(payload, p, collector);
            }
            alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload payload, Context ctx, Collector<Payload> collector) throws Exception {
        if(aliveSet.value() == null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        Payload tmp = new Payload(payload);
        tmp.type = "Tmp";
        tmp.key = 0;

        if(isValid(payload)){
            if("Insert".equals(payload.type)){
                if(aliveCount.value() == 1){
                    if(aliveSet.value().add(tmp)){
                        payload.type = "Add";
                        outputPayload(payload, alivePayload.value(), collector);
                    }
                } else {
                    aliveSet.value().add(tmp);
                }
            } else if("Delete".equals(payload.type)){
                if(aliveCount.value() == 1){
                    if(aliveSet.value().remove(tmp)){
                        payload.type = "Sub";
                        outputPayload(payload, alivePayload.value(), collector);
                    }
                } else {
                    aliveSet.value().remove(tmp);
                }
            }
        }
    }

    public void outputPayload(Payload left, Payload right, Collector<Payload> out) {
        Payload tmp = new Payload(left);
        if(right != null) {
            for(int i=0; i<right.attribute_name.size(); i++){
                String attr = right.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(right.attribute_value.get(i));
                }
            }
        }
        tmp.setKey(nextKey);
        out.collect(tmp);
    }
}
