package org.example;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;


public class UnicastOperator implements RichFunction {

    private long[] subtaskCounts;
    @Override
    public void open(Configuration configuration) throws Exception {
        subtaskCounts = new long[getRuntimeContext().getNumberOfParallelSubtasks()];

    }

    @Override
    public void close() throws Exception {


    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return null;
    }

    @Override
    public IterationRuntimeContext getIterationRuntimeContext() {
        return null;
    }

    @Override
    public void setRuntimeContext(RuntimeContext runtimeContext) {

    }
}
