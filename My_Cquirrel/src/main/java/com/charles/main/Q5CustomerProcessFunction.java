package com.charles.main;

import com.charles.RelationType.Payload;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Q5CustomerProcessFunction extends KeyedCoProcessFunction<Object, Payload, Payload, Payload> {
    // nextKey = C_CUSTKEY
    String nextKey = "C_CUSTKEY";
    ValueState<Integer> aliveCount;
    ValueState<Payload> alivePayload;
    ValueState<Set<Payload>> aliveSet;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Integer> countDesc = new ValueStateDescriptor<>("Q5CustomerCount", Integer.class);
        aliveCount = getRuntimeContext().getState(countDesc);

        ValueStateDescriptor<Payload> plDesc = new ValueStateDescriptor<>("Q5CustomerAlivePayload", Payload.class);
        alivePayload = getRuntimeContext().getState(plDesc);

        TypeInformation<Set<Payload>> ti= TypeInformation.of(new TypeHint<Set<Payload>>(){});
        ValueStateDescriptor<Set<Payload>> setDesc = new ValueStateDescriptor<>("Q5CustomerSet", ti);
        aliveSet = getRuntimeContext().getState(setDesc);
    }

    @Override
    public void processElement1(Payload nation, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        if("SetLive".equals(nation.type)){
            alivePayload.update(new Payload(nation));
            aliveCount.update(aliveCount.value()+1);
            for(Payload p: aliveSet.value()){
                nation.type="SetLive";
                outputPayload(nation,p,out);
            }
        } else {
            aliveCount.update(aliveCount.value()-1);
            for(Payload p: aliveSet.value()){
                nation.type="SetDead";
                outputPayload(nation,p,out);
            }
            alivePayload.update(null);
        }
    }

    @Override
    public void processElement2(Payload customer, Context ctx, Collector<Payload> out) throws Exception {
        if(aliveSet.value()==null){
            aliveSet.update(Collections.newSetFromMap(new ConcurrentHashMap<>()));
            aliveCount.update(0);
            alivePayload.update(null);
        }

        Payload tmp = new Payload(customer);
        tmp.type="Tmp";
        tmp.key=0;

        // 无额外过滤条件
        if("Insert".equals(customer.type)){
            if(aliveCount.value()==1){
                if(aliveSet.value().add(tmp)){
                    customer.type="SetLive";
                    outputPayload(alivePayload.value(),customer,out);
                }
            } else {
                aliveSet.value().add(tmp);
            }
        } else if("Delete".equals(customer.type)){
            if(aliveCount.value()==1){
                if(aliveSet.value().remove(tmp)){
                    customer.type="SetDead";
                    outputPayload(alivePayload.value(),customer,out);
                }
            } else {
                aliveSet.value().remove(tmp);
            }
        }
    }

    public void outputPayload(Payload nation, Payload customer, Collector<Payload> out) {
        Payload tmp = new Payload(customer);
        if(nation!=null){
            for(int i=0;i<nation.attribute_name.size();i++){
                String attr = nation.attribute_name.get(i);
                if(!tmp.attribute_name.contains(attr)){
                    tmp.attribute_name.add(attr);
                    tmp.attribute_value.add(nation.attribute_value.get(i));
                }
            }
        }
        tmp.setKey(nextKey);
        out.collect(tmp);
    }
}
