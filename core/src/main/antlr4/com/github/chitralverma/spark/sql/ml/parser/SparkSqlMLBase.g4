/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * This file contains code from the Apache Spark project (original license above).
 * It contains modifications, which are licensed as follows:
 */

/*
 *   Copyright (2021) Chitral Verma
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

grammar SparkSqlMLBase;
import SqlBase;

singleStatement
    : statement ';'* EOF                        #sparkSQLStatement
    | mlStatement ';'* EOF                      #sparkSQLMLStatement
    ;

mlStatement
    : fitEstimatorHeader TO '(' dataSetQuery=queryNoInsert ')'
        (WITH OPTIONS options=tablePropertyList)? (WITH HYPERPARAMS hyperParams=tablePropertyList)?
        storedAtLocation                                                                                   #fitEstimator
    ;

fitEstimatorHeader
    : FIT (AND overwrite=REPLACE)? A estimator=STRING ESTIMATOR
    ;

queryNoInsert
    : queryTerm queryOrganization
    ;

storedAtLocation
    : STORED AT locationSpec
    ;

//============================
// Start of the ML keywords list
//============================
FIT: 'FIT';
A: 'A';
ESTIMATOR: 'ESTIMATOR';
HYPERPARAMS: 'HYPERPARAMS';
DEFAULT: 'DEFAULT';