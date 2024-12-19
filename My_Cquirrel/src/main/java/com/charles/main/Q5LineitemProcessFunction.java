package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q5LineitemProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {

    String nextKey = "N_NAME"; // 为最终聚合使用，这里先保留全部字段

    ValueState<Integer> aliveCount;
    ValueState<Payload> alivePayload;
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("Q5LineitemCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Payload> plDesc = new ValueStateDescriptor<>("Q5LineitemAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(plDesc);

        TypeInformation<Set<Payload>> ti=TypeInformation.of(new TypeHint<Set<Payload>>(){});
        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q5LineitemSet", ti);
        aliveSet = getRuntimeContext().getState(setDesc);
    }

    public boolean isValid(Payload lineitem) {
        return true;
    }

    @Override
    public void processElement1(Payload interPayload, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if("SetLive".equals(interPayload.type)){
            alivePayload.update(new Payload(interPayload));
            aliveCount.update(aliveCount.value()+1);
            for(Payload p: aliveSet.value()){
                p.type="Add";
                outputPayload(interPayload,p,out);
            }
        } else if("SetDead".equals(interPayload.type)){
            aliveCount.update(aliveCount.value()-1);
            for(Payload p: aliveSet.value()){
                p.type="Sub";
                outputPayload(interPayload,p,out);
            }
            alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload lineitem, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }
        Payload tmp = new Payload(lineitem); tmp.type="Tmp"; tmp.key=0;

        if(isValid(lineitem)){
            if("Insert".equals(lineitem.type)){
                if(aliveCount.value()==1){
                    if(aliveSet.value().add(tmp)){
                        lineitem.type="Add";
                        outputPayload(alivePayload.value(),lineitem,out);
                    }
                } else {
                    aliveSet.value().add(tmp);
                }
            } else if("Delete".equals(lineitem.type)){
                if(aliveCount.value()==1){
                    if(aliveSet.value().remove(tmp)){
                        lineitem.type="Sub";
                        outputPayload(alivePayload.value(),lineitem,out);
                    }
                } else {
                    aliveSet.value().remove(tmp);
                }
            }
        }
    }

    public void outputPayload(Payload inter, Payload lineitem, Collector<Payload> out) {
        Payload tmp = new Payload(lineitem);
        if(inter!=null){
            for(int i=0;i<inter.attribute_name.size();i++){
                String attr = inter.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(inter.attribute_value.get(i));
                }
            }
        }

        tmp.setKey("N_NAME");
        out.collect(tmp);
    }
}
