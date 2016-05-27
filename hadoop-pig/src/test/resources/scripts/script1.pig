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

raw = LOAD '../data/excite-small copy.log' USING PigStorage('	') AS (username, time, query);

--gen1= foreach raw generate user,count(user) as count;

clean1 =filter raw by query is not null; 
dump clean1;

gen1 = foreach clean1 generate username,query;
dump gen1;

gen2= group gen1 by username;
dump gen2;

--user1 = group gen by user;


-- dump user1;

-- Use the PigStorage function to store the results. 
-- Output: (hour, n-gram, score, count, average_counts_among_all_hours)
-- STORE user INTO 'script1-local-copy-results1.txt' USING PigStorage();
