package com.xuwuji.hadoop.counter;

/**
 * After every map reduce execution, you will see a set of system defined
 * counters getting published, such as File System counters, Job counters, and
 * Map Reduce Framework counters. These counters help us understand the
 * execution in detail. They give very detailed information about the number of
 * bytes written to HDFS, read from HDFS, the input given to a map, the output
 * received from a map, and so on. Similar to this information, we can also add
 * our own user-defined counters, which will help us track the execution in a
 * better manner.
 * 
 * @author wuxu
 *
 */
public enum RecordCounter {

	JSON_RECORD_COUNT
}
