package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q5NationProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    String nextKey = "N_NATIONKEY";

    ValueState<Integer> aliveCount;
    ValueState<Payload> alivePayload;
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("Q5NationCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Payload> plDesc = new ValueStateDescriptor<>("Q5NationAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(plDesc);

        TypeInformation<Set<Payload>> ti = TypeInformation.of(new TypeHint<Set<Payload>>() {});
        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q5NationSet", ti);
        aliveSet = getRuntimeContext().getState(setDesc);
    }

    public boolean isValid(Payload nation, Payload region) {
        return true;
    }

    @Override
    public void processElement1(Payload value, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }
        if("SetLive".equals(value.type)){
            System.out.println("[LINZIDONG]Q5NationProcessFunction - region is SetLive");
            alivePayload.update(new Payload(value));
            aliveCount.update(aliveCount.value()+1);
            for(Payload p: aliveSet.value()){
                value.type="SetLive";
                outputPayload(value,p,out);
            }
        } else {
            System.out.println("[LINZIDONG]Q5NationProcessFunction - region is SetDead");
            aliveCount.update(aliveCount.value()-1);
            for(Payload p: aliveSet.value()){
                value.type="SetDead";
                outputPayload(value,p,out);
            }
            alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload value, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }
        Payload tmp = new Payload(value); tmp.type="Tmp"; tmp.key=0;

        if("Insert".equals(value.type)){
            System.out.println("[LINZIDONG]Q5NationProcessFunction - Insert into nation");
            if(aliveCount.value()==1){
                if(aliveSet.value().add(tmp)){
                    value.type="SetLive";
                    outputPayload(alivePayload.value(),value,out);
                }
            } else {
                aliveSet.value().add(tmp);
            }
        } else if("Delete".equals(value.type)){
            System.out.println("[LINZIDONG]Q5NationProcessFunction - Delte from nation");
            if(aliveCount.value()==1){
                if(aliveSet.value().remove(tmp)){
                    value.type="SetDead";
                    outputPayload(alivePayload.value(),value,out);
                }
            } else {
                aliveSet.value().remove(tmp);
            }
        }
    }

    public void outputPayload(Payload region, Payload nation, Collector<Payload> out) {
        Payload tmp = new Payload(nation);
        if(region!=null){
            for(int i=0;i<region.attribute_name.size();i++){
                String attr = region.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(region.attribute_value.get(i));
                }
            }
        }
        tmp.setKey(nextKey);
        out.collect(tmp);
    }
}
