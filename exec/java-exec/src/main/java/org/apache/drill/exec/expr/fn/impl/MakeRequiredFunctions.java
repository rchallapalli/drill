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
package org.apache.drill.exec.expr.fn.impl;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.DateHolder;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28DenseHolder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38DenseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableBigIntHolder;
import org.apache.drill.exec.expr.holders.NullableBitHolder;
import org.apache.drill.exec.expr.holders.NullableDateHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal18Holder;
import org.apache.drill.exec.expr.holders.NullableDecimal28DenseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38DenseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal9Holder;
import org.apache.drill.exec.expr.holders.NullableFloat4Holder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.NullableIntHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalDayHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableTimeHolder;
import org.apache.drill.exec.expr.holders.NullableTimeStampHolder;
import org.apache.drill.exec.expr.holders.NullableVarBinaryHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.TimeHolder;
import org.apache.drill.exec.expr.holders.TimeStampHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.record.RecordBatch;


/*
 * This class implements functions which can be used to convert a nullable type to a non-nullable type
 * and throws an exception when it encounters a null value
 * Sample Usage :
 *     create table fewtypes_required as select makerequired(int_col) req_int_col from `fewtypes.json`;
 * The above query makes sure that the column written to the parquet file is a required column
 *
 * TODO : Add a feature to replace nulls with a user-defined value
 */
public class MakeRequiredFunctions {

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredInt implements DrillSimpleFunc {

    @Param
    NullableIntHolder input;
    @Output
    IntHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredVarchar implements DrillSimpleFunc {

    @Param
    NullableVarCharHolder input;
    @Output
    VarCharHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.buffer = input.buffer;
        output.start = input.start;
        output.end = input.end;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredVarbinary implements DrillSimpleFunc {

    @Param
    NullableVarBinaryHolder input;
    @Output
    VarBinaryHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.buffer = input.buffer;
        output.start = input.start;
        output.end = input.end;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredBoolean implements DrillSimpleFunc {

    @Param
    NullableBitHolder input;
    @Output
    BitHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }


  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredBigInt implements DrillSimpleFunc {

    @Param
    NullableBigIntHolder input;
    @Output
    BigIntHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }


  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredFloat4 implements DrillSimpleFunc {

    @Param
    NullableFloat4Holder input;
    @Output
    Float4Holder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredFloat8 implements DrillSimpleFunc {

    @Param
    NullableFloat8Holder input;
    @Output
    Float8Holder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDate implements DrillSimpleFunc {

    @Param
    NullableDateHolder input;
    @Output
    DateHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredTime implements DrillSimpleFunc {

    @Param
    NullableTimeHolder input;
    @Output
    TimeHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredIntervalDay implements DrillSimpleFunc {

    @Param
    NullableIntervalDayHolder input;
    @Output
    IntervalDayHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.days = input.days;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredIntervalYear implements DrillSimpleFunc {

    @Param
    NullableIntervalYearHolder input;
    @Output
    IntervalYearHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }
  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredTimeStamp implements DrillSimpleFunc {

    @Param
    NullableTimeStampHolder input;
    @Output
    TimeStampHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDecimal9 implements DrillSimpleFunc {

    @Param
    NullableDecimal9Holder input;
    @Output
    Decimal9Holder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDecimal18 implements DrillSimpleFunc {

    @Param
    NullableDecimal18Holder input;
    @Output
    Decimal18Holder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.value = input.value;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDecimal28Sparse implements DrillSimpleFunc {

    @Param
    NullableDecimal28SparseHolder input;
    @Output
    Decimal28SparseHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.buffer = input.buffer;
        output.start = input.start;
        output.precision = input.precision;
        output.scale = input.scale;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDecimal28Dense implements DrillSimpleFunc {

    @Param
    NullableDecimal28DenseHolder input;
    @Output
    Decimal28DenseHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.buffer = input.buffer;
        output.start = input.start;
        output.precision = input.precision;
        output.scale = input.scale;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDecimal38Dense implements DrillSimpleFunc {

    @Param
    NullableDecimal38DenseHolder input;
    @Output
    Decimal38DenseHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.buffer = input.buffer;
        output.start = input.start;
        output.precision = input.precision;
        output.scale = input.scale;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

  @FunctionTemplate(name = "makerequired", scope = FunctionScope.SIMPLE, nulls = NullHandling.INTERNAL)
  public static class MakeRequiredDecimal38Sparse implements DrillSimpleFunc {

    @Param
    NullableDecimal38SparseHolder input;
    @Output
    Decimal38SparseHolder output;

    public void setup() {
    }

    public void eval() {
      if (input.isSet == 1) {
        output.buffer = input.buffer;
        output.start = input.start;
        output.precision = input.precision;
        output.scale = input.scale;
      } else {
        throw org.apache.drill.common.exceptions.UserException.functionError()
          .message("You tried to make a column required when it has null values")
          .build();
      }
    }

  }

}

