package com.at.writehive;

import org.apache.flink.connector.file.table.PartitionCommitPolicy;

/**
 * @create 2022-08-14
 */
public class FlinkWriteHiveConsumerPartitionCommitPolicy {

    public static void main(String[] args) {

    }


    static class ConsumerPartitionCommitPolicy implements PartitionCommitPolicy{

        @Override
        public void commit(Context context) throws Exception {

        }



    }


}