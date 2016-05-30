/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

-- Query Phrase Popularity (local mode)

-- This script processes a search query log file from the Excite search engine and finds search phrases that occur with particular high frequency during certain times of the day.

-- Register the tutorial JAR file so that the included UDFs can be called in the script.
-- REGISTER ./tutorial.jar;

-- Use the PigStorage function to load the excite log file into the raw bag as an array of records.

-- Input: (user,time,query) 

-- 通过分隔符，每条记录有三个field：username, time, query
raw = LOAD '../data/sample.log' USING PigStorage('	') AS (username, time, query);
-- filter掉query为空的所有记录
clean1 =filter raw by query is not null; 
dump clean1;
-- 每条记录只需要username和query这两个field
gen1 = foreach clean1 generate username,query;
dump gen1;
-- 通过username来group所有的记录，collects all records with the same value for the provided key together into a bag.
-- 每个key对应的是一群object，在这里每个username对应了gen1形式的记录
-- Therecordscomingoutofthegroup bystatementhavetwofields,thekeyandthebag of collected records.
-- The key field is named group.The bag is named for the alias that was grouped.
gen2= group gen1 by username;
-- gen2:{group:bytearray,gen1:bag{:tuple(username:bytearray,query:bytearray)}}
describe gen2;
dump gen2;
-- 对于gen2（group，gen1）的每条记录进行count操作
count= foreach gen2 generate group,COUNT(gen1);
dump count;

--group is the first operator we have looked at that usually will force a reduce phase. 
--Grouping means collecting all records where the key has the same value. 
--If the pipeline is in a map phase, this will force it to shuffle and then reduce. 
--If the pipeline is already in a reduce, this will force it to pass through map, shuffle, and reduce phases.
-- Finally, group handles nulls in the same way that SQL handles them: by collecting all records with a null key into the same group. 
-- 对query进行group再count，相当于算distinct
gen3 = foreach clean1 generate query;
group1= group gen3 by query;
dump group1;
count1 =foreach group1 generate group as query, COUNT(gen3) as countNum;

--t is also possible to reverse the order of the sort by appending desc to a key in the sort.
-- In order statements with multiple keys, desc applies only to the key it immediately follows.
-- Other keys will still be sorted in ascending order.
order_count= order count1 by countNum desc;
dump order_count;

--The distinct statement is very simple. It removes duplicate records. It works only on entire records, not on individual fields.
distinctData= distinct raw;
dump distinctData;

-- A left outer join means records from the left side will be included even when they do not have a match on the right side.
-- Likewise, a right outer joins means records from the right side will be included even when they do not have a match on the left side.
-- A full outer join means records from both sides are taken even when they do not have matches.
raw2= load '../data/sample2.log' as (username,time,keyword);

-- This is done by indicating keys for each input.
-- When those keys are equal,# the two rows are joined. Records for which no match is found are dropped.
jnd1= join raw by username, raw2 by username;
dump jnd1;

jnd2 = join raw by username left outer, raw2 by username;
dump jnd2;

first10= limit jnd2 10;
dump first10;

-- A foreach with a flatten produces a cross product of every record in the bag with all of the other expressions in the generate statement. 

--cogroup is a generalization of group. Instead of collecting records of one input based on a key, it collects records of n inputs based on a key. The result is a record with a key and one bag for each input. Each bag contains all records from that input that have the given value for the key:
-- A = load 'input1' as (id:int, val:float); B = load 'input2' as (id:int, val2:int); C = cogroup A by id, B by id;
-- describe C;
-- C: {group: int,A: {id: int,val: float},B: {id: int,val2: int}}

-- Use the PigStorage function to store the results. 
-- Output: (hour, n-gram, score, count, average_counts_among_all_hours)
-- STORE user INTO 'script1-local-copy-results1.txt' USING PigStorage();
