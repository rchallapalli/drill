/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.fn.impl;

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.exec.store.ischema.InfoSchemaFilter;
import org.junit.Test;


public class TestMakeRequired extends BaseTestQuery {

  @Test
  public void testRequiredTypes1() throws Exception {
    TypeProtos.MajorType.Builder builder = TypeProtos.MajorType.getDefaultInstance().toBuilder();
    builder.setMinorType(TypeProtos.MinorType.INT);
    builder.setMode(TypeProtos.DataMode.REQUIRED);
    TypeProtos.MajorType majorTypeInt = builder.build();
    builder.setMinorType(TypeProtos.MinorType.BIGINT);
    TypeProtos.MajorType majorTypeBigint = builder.build();
    builder.setMinorType(TypeProtos.MinorType.DATE);
    TypeProtos.MajorType majorTypeDate = builder.build();
    builder.setMinorType((TypeProtos.MinorType.TIME));
    TypeProtos.MajorType majorTypeTime = builder.build();
    builder.setMinorType(TypeProtos.MinorType.TIMESTAMP);
    TypeProtos.MajorType majorTypeTimestamp = builder.build();
    builder.setMinorType(TypeProtos.MinorType.INTERVALDAY);
    TypeProtos.MajorType majorTypeIntervalDay = builder.build();
    builder.setMinorType(TypeProtos.MinorType.INTERVALYEAR);
    TypeProtos.MajorType majorTypeIntervalYear = builder.build();
    builder.setMinorType(TypeProtos.MinorType.VARCHAR);
    TypeProtos.MajorType majorTypeVarchar = builder.build();
    builder.setMinorType(TypeProtos.MinorType.FLOAT4);
    TypeProtos.MajorType majorTypeFloat = builder.build();
    builder.setMinorType(TypeProtos.MinorType.FLOAT8);
    TypeProtos.MajorType majorTypeFloat8 = builder.build();
    builder.setMinorType(TypeProtos.MinorType.BIT);
    TypeProtos.MajorType majorTypeBoolean = builder.build();

    String query = "select " +
      "makerequired(cast (int_col as int)) req_int_col, " +
      "makerequired(bigint_col) as req_bigint_col, " +
      "makerequired(cast(date_col as date)) req_date_col, " +
      "makerequired(cast(time_col as time)) req_time_col, " +
      "makerequired(cast(timestamp_col as timestamp)) req_timestamp_col, " +
      "makerequired(cast(interval_col as interval day)) req_interval_day_col, " +
      "makerequired(cast(interval_col as interval year)) req_interval_year_col, " +
      "makerequired(varchar_col) req_varchar_col, " +
      "makerequired(cast(float_col as float)) req_float_col, " +
      "makerequired(cast(double_col as double)) req_double_col, " +
      "makerequired(cast(bool_col as boolean)) req_bool_col " +
      "from cp.`jsoninput/fewtypes.json` d";
    testBuilder()
      .sqlQuery(query)
      .unOrdered()
      .csvBaselineFile("functions/make_required/test1.csv")
      .baselineTypes(
        majorTypeInt,
        majorTypeBigint,
        majorTypeDate,
        majorTypeTime,
        majorTypeTimestamp,
        majorTypeIntervalDay,
        majorTypeIntervalYear,
        majorTypeVarchar,
        majorTypeFloat,
        majorTypeFloat8,
        majorTypeBoolean)
      .baselineColumns(
        "req_int_col",
        "req_bigint_col",
        "req_date_col",
        "req_time_col",
        "req_timestamp_col",
        "req_interval_day_col",
        "req_interval_year_col",
        "req_varchar_col",
        "req_float_col",
        "req_double_col",
        "req_bool_col")
      .compareHeader()
      .build().run();
  }


    /*
     *  1. Test With a non-existent column
     */
}
