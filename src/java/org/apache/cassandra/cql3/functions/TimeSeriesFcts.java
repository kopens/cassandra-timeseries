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
package org.apache.cassandra.cql3.functions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.clearspring.analytics.stream.cardinality.ICardinality;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.cql3.FunctionContext;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.NumberType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.utils.MurmurHash;

import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/**
 * Native functions specialised for time-series workloads.
 *
 * <p>This family provides:
 * <ul>
 *     <li>{@code time_bucket(duration, timestamp [, origin])} - a downsampling/bucketing scalar that rounds a
 *     timestamp down to the start of the time window of the given width. It is the time-series ergonomic spelling of
 *     the existing {@code floor(timestamp, duration [, origin])} function (note the swapped argument order, matching
 *     the convention used by other time-series databases).</li>
 *     <li>{@code first(value, timestamp)} / {@code last(value, timestamp)} - aggregates returning the {@code value}
 *     paired with the smallest/largest {@code timestamp} seen within a group. Both arguments are kept as raw bytes so
 *     the functions are generic over any {@code value} type; timestamps are ordered using the timestamp argument's own
 *     comparator, which makes the aggregates safe to merge across replicas (the operation is associative and the
 *     result is independent of row arrival order).</li>
 * </ul>
 */
public final class TimeSeriesFcts
{
    public static void addFunctionsTo(NativeFunctions functions)
    {
        // time_bucket(duration, timestamp) and time_bucket(duration, timestamp, origin)
        functions.add(new TimeBucketFunction(false));
        functions.add(new TimeBucketFunction(true));

        // time_bucket_gapfill(duration, timestamp, start, finish): buckets like time_bucket (aligned to start) and
        // additionally declares the dense [start, finish) range that gap-filling will materialize.
        functions.add(new TimeBucketGapfillFunction());

        // first(value, timestamp) / last(value, timestamp) for any value type
        functions.add(new FunctionFactory("first",
                                           FunctionParameter.anyType(true),
                                           FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeFirstLastFunction("first", argTypes.get(0), argTypes.get(1), true);
            }
        });
        functions.add(new FunctionFactory("last",
                                          FunctionParameter.anyType(true),
                                          FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeFirstLastFunction("last", argTypes.get(0), argTypes.get(1), false);
            }
        });

        // delta / rate / derivative over a numeric (value, timestamp) series
        functions.add(numericSeriesFactory("delta", SeriesFunction.DELTA));
        functions.add(numericSeriesFactory("rate", SeriesFunction.RATE));
        functions.add(numericSeriesFactory("derivative", SeriesFunction.DERIVATIVE));

        // percentile(value, q): exact continuous percentile (linear interpolation) of a numeric column
        functions.add(new FunctionFactory("percentile",
                                          FunctionParameter.anyType(false),
                                          FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (!(argTypes.get(0) instanceof NumberType))
                    throw invalidRequest("Function percentile requires a numeric value argument, but found type %s",
                                         argTypes.get(0).asCQL3Type());
                if (!(argTypes.get(1) instanceof NumberType))
                    throw invalidRequest("Function percentile requires a numeric quantile argument, but found type %s",
                                         argTypes.get(1).asCQL3Type());

                return makePercentileFunction(argTypes.get(0), argTypes.get(1));
            }
        });

        // time_weighted_average(value, ts): trapezoidal time-weighted mean; integral(value, ts): area (value-seconds)
        functions.add(timeIntegralFactory("time_weighted_average", true));
        functions.add(timeIntegralFactory("integral", false));

        // variance / stddev (sample) of a numeric column
        functions.add(varianceFactory("variance", false));
        functions.add(varianceFactory("stddev", true));

        // counter_delta / counter_rate: increase of a monotonic counter over (value, timestamp), compensating for
        // resets (a drop in value is treated as the counter restarting from 0).
        functions.add(counterFactory("counter_delta", false));
        functions.add(counterFactory("counter_rate", true));

        // Two-variable statistics over (y, x): correlation, covariance, and linear regression.
        functions.add(bivariateFactory("corr", BivariateStat.CORR));
        functions.add(bivariateFactory("covar_pop", BivariateStat.COVAR_POP));
        functions.add(bivariateFactory("covar_samp", BivariateStat.COVAR_SAMP));
        functions.add(bivariateFactory("regr_slope", BivariateStat.REGR_SLOPE));
        functions.add(bivariateFactory("regr_intercept", BivariateStat.REGR_INTERCEPT));
        functions.add(bivariateFactory("regr_r2", BivariateStat.REGR_R2));

        // histogram(value, min, max, nbuckets): frequency counts, with underflow/overflow buckets
        functions.add(new FunctionFactory("histogram",
                                          FunctionParameter.anyType(false),
                                          FunctionParameter.anyType(false),
                                          FunctionParameter.anyType(false),
                                          FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                for (int i = 0; i < 4; i++)
                    if (!(argTypes.get(i) instanceof NumberType))
                        throw invalidRequest("Function histogram requires numeric arguments, but argument %d has type %s",
                                             i + 1, argTypes.get(i).asCQL3Type());

                return makeHistogramFunction(argTypes);
            }
        });

        // approx_count_distinct(value): HyperLogLog cardinality estimate over any value type
        functions.add(new FunctionFactory("approx_count_distinct", FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                return makeApproxCountDistinctFunction(argTypes.get(0));
            }
        });

        // locf(value): identity for real rows; a marker that tells time_bucket_gapfill to carry the previous
        // non-empty bucket's value forward into synthesized empty buckets (last-observation-carried-forward).
        functions.add(new FunctionFactory("locf", FunctionParameter.anyType(true))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                AbstractType<?> type = argTypes.get(0);
                return new NativeScalarFunction("locf", type, type)
                {
                    @Override
                    public Arguments newArguments(FunctionContext context)
                    {
                        return FunctionArguments.newNoopInstance(context, 1);
                    }

                    @Override
                    public ByteBuffer execute(Arguments arguments)
                    {
                        return arguments.get(0);
                    }
                };
            }
        });

        // interpolate(value): on real rows returns the value as a double; a marker that tells time_bucket_gapfill to
        // linearly interpolate empty buckets between the surrounding non-empty buckets.
        functions.add(new FunctionFactory("interpolate", FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (!(argTypes.get(0) instanceof NumberType))
                    throw invalidRequest("Function interpolate requires a numeric argument, but found type %s",
                                         argTypes.get(0).asCQL3Type());

                return new NativeScalarFunction("interpolate", DoubleType.instance, argTypes.get(0))
                {
                    @Override
                    public ByteBuffer execute(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        return value == null ? null : DoubleType.instance.decompose(value.doubleValue());
                    }
                };
            }
        });
    }

    /**
     * Builds the {@code approx_count_distinct(value)} aggregate: an estimate of the number of distinct values using a
     * HyperLogLog++ sketch (the same {@code stream-lib} estimator used for SSTable cardinality). Memory is bounded
     * regardless of cardinality, and the sketch merges associatively so the result is order-independent. Returns 0 for
     * an empty group. Distinct values are identified by their serialized bytes.
     *
     * @param valueType the type of the value argument
     */
    private static NativeAggregateFunction makeApproxCountDistinctFunction(AbstractType<?> valueType)
    {
        return new NativeAggregateFunction("approx_count_distinct", LongType.instance, valueType)
        {
            @Override
            public Arguments newArguments(FunctionContext context)
            {
                // The value is hashed as raw bytes, so no deserialization is needed.
                return FunctionArguments.newNoopInstance(context, 1);
            }

            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    // p=14 gives ~0.8% standard error with a fixed ~16KB footprint.
                    private ICardinality cardinality = new HyperLogLogPlus(14, 25);

                    @Override
                    public void reset()
                    {
                        cardinality = new HyperLogLogPlus(14, 25);
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        ByteBuffer value = arguments.get(0);
                        if (value == null)
                            return;

                        // Hash the buffer's bytes in place (no array copy) and feed the pre-hashed value to the sketch.
                        long hash = MurmurHash.hash2_64(value, value.position(), value.remaining(), 0);
                        cardinality.offerHashed(hash);
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        return LongType.instance.decompose(cardinality.cardinality());
                    }
                };
            }
        };
    }

    /** Builds the {@code variance}/{@code stddev} aggregate factory (sample, i.e. divided by n-1). */
    private static FunctionFactory varianceFactory(String functionName, boolean sqrt)
    {
        return new FunctionFactory(functionName, FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (!(argTypes.get(0) instanceof NumberType))
                    throw invalidRequest("Function %s requires a numeric argument, but found type %s",
                                         functionName, argTypes.get(0).asCQL3Type());

                return makeVarianceFunction(functionName, sqrt, argTypes.get(0));
            }
        };
    }

    /** Builds the {@code counter_delta}/{@code counter_rate} factory over a numeric (value, timestamp) series. */
    private static FunctionFactory counterFactory(String functionName, boolean perSecond)
    {
        return new FunctionFactory(functionName,
                                   FunctionParameter.anyType(false),
                                   FunctionParameter.fixed(CQL3Type.Native.TIMESTAMP, CQL3Type.Native.BIGINT))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (!(argTypes.get(0) instanceof NumberType))
                    throw invalidRequest("Function %s requires a numeric value argument, but found type %s",
                                         functionName, argTypes.get(0).asCQL3Type());

                return makeCounterFunction(functionName, perSecond, argTypes.get(0), argTypes.get(1));
            }
        };
    }

    /** The bivariate statistic computed by {@link #makeBivariateFunction}. */
    private enum BivariateStat { CORR, COVAR_POP, COVAR_SAMP, REGR_SLOPE, REGR_INTERCEPT, REGR_R2 }

    /** Builds the {@code corr}/{@code covar_pop}/{@code covar_samp} factory over two numeric columns {@code (y, x)}. */
    private static FunctionFactory bivariateFactory(String functionName, BivariateStat kind)
    {
        return new FunctionFactory(functionName,
                                   FunctionParameter.anyType(false),
                                   FunctionParameter.anyType(false))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (!(argTypes.get(0) instanceof NumberType) || !(argTypes.get(1) instanceof NumberType))
                    throw invalidRequest("Function %s requires two numeric arguments, but found types %s and %s",
                                         functionName, argTypes.get(0).asCQL3Type(), argTypes.get(1).asCQL3Type());

                return makeBivariateFunction(functionName, kind, argTypes.get(0), argTypes.get(1));
            }
        };
    }

    /**
     * Builds the {@link FunctionFactory} for {@code delta}/{@code rate}/{@code derivative}, validating that the value
     * argument is numeric and the timestamp argument is a {@code timestamp} or {@code bigint}.
     */
    private static FunctionFactory numericSeriesFactory(String functionName, SeriesFunction kind)
    {
        return new FunctionFactory(functionName,
                                   FunctionParameter.anyType(false),
                                   FunctionParameter.fixed(CQL3Type.Native.TIMESTAMP, CQL3Type.Native.BIGINT))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                AbstractType<?> valueType = argTypes.get(0);
                if (!(valueType instanceof NumberType))
                    throw invalidRequest("Function %s requires a numeric value argument, but found type %s",
                                         functionName, valueType.asCQL3Type());

                return makeSeriesFunction(functionName, kind, valueType, argTypes.get(1));
            }
        };
    }

    /**
     * {@code time_bucket(duration, timestamp [, origin])}: rounds {@code timestamp} down to the closest multiple of
     * {@code duration}, optionally offset by {@code origin}. Equivalent to {@code floor(timestamp, duration [, origin])}
     * but with the (duration, timestamp) argument order used by time-series databases.
     */
    private static final class TimeBucketFunction extends NativeScalarFunction
    {
        private final boolean withOrigin;

        private TimeBucketFunction(boolean withOrigin)
        {
            super("time_bucket",
                  TimestampType.instance,
                  argTypes(withOrigin));
            this.withOrigin = withOrigin;
        }

        private static AbstractType<?>[] argTypes(boolean withOrigin)
        {
            return withOrigin
                   ? new AbstractType<?>[]{ DurationType.instance, TimestampType.instance, TimestampType.instance }
                   : new AbstractType<?>[]{ DurationType.instance, TimestampType.instance };
        }

        @Override
        protected boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters)
        {
            // time_bucket is monotonic in its timestamp argument as long as the duration (and optional origin) are
            // constants. This mirrors floor() and is what allows time_bucket to be used as a GROUP BY selector.
            return partialParameters.get(1) == UNRESOLVED
                   && partialParameters.get(0) != UNRESOLVED
                   && (partialParameters.size() == 2 || partialParameters.get(2) != UNRESOLVED);
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.containsNulls())
                return null;

            Duration duration = arguments.get(0);
            long time = arguments.getAsLong(1);
            long origin = withOrigin ? arguments.getAsLong(2) : 0L;

            if (!duration.hasMillisecondPrecision())
                throw invalidRequest("The time_bucket cannot be computed for the %s duration as precision is below " +
                                     "1 millisecond", duration);

            long floor = Duration.floorTimestamp(time, duration, origin);
            return TimestampType.instance.fromTimeInMillis(floor);
        }
    }

    /**
     * {@code time_bucket_gapfill(duration, timestamp, start, finish)}: computes the same bucket as
     * {@code time_bucket(duration, timestamp, start)} (buckets aligned to {@code start}), and additionally declares the
     * dense {@code [start, finish)} range over which gap-filling will materialize a row for every bucket. As a plain
     * scalar this returns the bucket start; the {@code start}/{@code finish} bounds are consumed by the gap-fill result
     * transform. Like {@code time_bucket} it is monotonic in its timestamp argument so it can be a {@code GROUP BY}
     * selector.
     */
    private static final class TimeBucketGapfillFunction extends NativeScalarFunction
    {
        private TimeBucketGapfillFunction()
        {
            super("time_bucket_gapfill",
                  TimestampType.instance,
                  DurationType.instance, TimestampType.instance, TimestampType.instance, TimestampType.instance);
        }

        @Override
        protected boolean isPartialApplicationMonotonic(List<ByteBuffer> partialParameters)
        {
            // Monotonic in the timestamp (arg 1) as long as the duration and the start/finish bounds are constants.
            return partialParameters.get(1) == UNRESOLVED
                   && partialParameters.get(0) != UNRESOLVED
                   && partialParameters.get(2) != UNRESOLVED
                   && partialParameters.get(3) != UNRESOLVED;
        }

        @Override
        public ByteBuffer execute(Arguments arguments) throws InvalidRequestException
        {
            if (arguments.containsNulls())
                return null;

            Duration duration = arguments.get(0);
            long time = arguments.getAsLong(1);
            long start = arguments.getAsLong(2);
            long finish = arguments.getAsLong(3);

            if (!duration.hasMillisecondPrecision())
                throw invalidRequest("The time_bucket_gapfill cannot be computed for the %s duration as precision is " +
                                     "below 1 millisecond", duration);
            if (finish <= start)
                throw invalidRequest("The time_bucket_gapfill finish must be greater than start");

            long floor = Duration.floorTimestamp(time, duration, start);
            return TimestampType.instance.fromTimeInMillis(floor);
        }
    }

    /**
     * Builds a {@code first}/{@code last} aggregate for a given (value, timestamp) signature.
     *
     * @param name the function name ({@code "first"} or {@code "last"})
     * @param valueType the type of the value to return
     * @param timestampType the type used to order rows within the group
     * @param keepEarliest {@code true} for {@code first} (smallest timestamp wins), {@code false} for {@code last}
     */
    private static NativeAggregateFunction makeFirstLastFunction(String name,
                                                                 AbstractType<?> valueType,
                                                                 AbstractType<?> timestampType,
                                                                 boolean keepEarliest)
    {
        return new NativeAggregateFunction(name, valueType, valueType, timestampType)
        {
            @Override
            public Arguments newArguments(FunctionContext context)
            {
                // Keep both arguments as raw bytes: the value is returned verbatim and timestamps are compared with the
                // timestamp argument's own (order-preserving) comparator.
                return FunctionArguments.newNoopInstance(context, 2);
            }

            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private ByteBuffer bestTimestamp;
                    private ByteBuffer bestValue;
                    private boolean hasValue;

                    @Override
                    public void reset()
                    {
                        bestTimestamp = null;
                        bestValue = null;
                        hasValue = false;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        return hasValue ? bestValue : null;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        ByteBuffer value = arguments.get(0);
                        ByteBuffer timestamp = arguments.get(1);

                        // A row with no timestamp cannot be ordered, so it is ignored.
                        if (timestamp == null)
                            return;

                        if (!hasValue || winsOver(timestamp))
                        {
                            bestTimestamp = timestamp;
                            bestValue = value;
                            hasValue = true;
                        }
                    }

                    private boolean winsOver(ByteBuffer timestamp)
                    {
                        // unwrap(), not the declared type: on WITH CLUSTERING ORDER BY (ts DESC) the
                        // clustering column's type is ReversedType, whose compare() inverts. Comparing
                        // with it makes first() return the LATEST row and last() the EARLIEST -- silently,
                        // on exactly the descending layout industrial tables use. "Earliest" must mean
                        // earliest in time regardless of how the table happens to be sorted on disk.
                        int cmp = timestampType.unwrap().compare(timestamp, bestTimestamp);
                        return keepEarliest ? cmp < 0 : cmp > 0;
                    }
                };
            }
        };
    }

    /** The kind of change-over-time computation performed by {@link #makeSeriesFunction}. */
    private enum SeriesFunction
    {
        /** Difference between the values at the largest and smallest timestamps. */
        DELTA,
        /** {@link #DELTA} divided by the elapsed time, expressed per second. */
        RATE,
        /** Least-squares regression slope of value over time, expressed per second. */
        DERIVATIVE
    }

    /**
     * Builds a {@code delta}/{@code rate}/{@code derivative} aggregate over a numeric {@code (value, timestamp)}
     * series, returning a {@code double}.
     *
     * <p>All three are computed from order-independent accumulators (timestamp endpoints for {@code delta}/{@code rate}
     * and the regression sums n, &Sigma;x, &Sigma;y, &Sigma;xy, &Sigma;x&sup2; for {@code derivative}), so the result
     * does not depend on the order in which rows are fed to the aggregate. Counter resets are not compensated for; these
     * functions use gauge semantics.
     *
     * @param name the function name
     * @param kind the computation to perform
     * @param valueType the numeric type of the value argument
     * @param timestampType the type of the timestamp argument ({@code timestamp} or {@code bigint})
     */
    private static NativeAggregateFunction makeSeriesFunction(String name,
                                                              SeriesFunction kind,
                                                              AbstractType<?> valueType,
                                                              AbstractType<?> timestampType)
    {
        return new NativeAggregateFunction(name, DoubleType.instance, valueType, timestampType)
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    // Endpoints used by delta and rate.
                    private long minTs;
                    private long maxTs;
                    private double valueAtMin;
                    private double valueAtMax;
                    // Least-squares accumulators used by derivative (x = seconds, y = value). x is measured relative to
                    // the first timestamp seen rather than the absolute epoch: the regression slope is invariant to
                    // shifting x by a constant, and keeping x small avoids the catastrophic cancellation that absolute
                    // epoch-second values (~1e9) would cause in (n*sumXX - sumX*sumX).
                    private long baseTs;
                    private long count;
                    private double sumX;
                    private double sumY;
                    private double sumXY;
                    private double sumXX;

                    @Override
                    public void reset()
                    {
                        minTs = 0;
                        maxTs = 0;
                        valueAtMin = 0;
                        valueAtMax = 0;
                        baseTs = 0;
                        count = 0;
                        sumX = 0;
                        sumY = 0;
                        sumXY = 0;
                        sumXX = 0;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        Number timestamp = arguments.get(1);

                        // A row missing the value or the timestamp cannot contribute to the series.
                        if (value == null || timestamp == null)
                            return;

                        double y = value.doubleValue();
                        long t = timestamp.longValue();

                        if (count == 0)
                            baseTs = t;

                        if (count == 0 || t < minTs)
                        {
                            minTs = t;
                            valueAtMin = y;
                        }
                        if (count == 0 || t > maxTs)
                        {
                            maxTs = t;
                            valueAtMax = y;
                        }

                        double x = (t - baseTs) / 1000.0;
                        sumX += x;
                        sumY += y;
                        sumXY += x * y;
                        sumXX += x * x;
                        count++;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        if (count == 0)
                            return null;

                        switch (kind)
                        {
                            case DELTA:
                                return DoubleType.instance.decompose(valueAtMax - valueAtMin);
                            case RATE:
                            {
                                long spanMillis = maxTs - minTs;
                                if (spanMillis == 0)
                                    return null;
                                return DoubleType.instance.decompose((valueAtMax - valueAtMin) / (spanMillis / 1000.0));
                            }
                            case DERIVATIVE:
                            {
                                double denominator = count * sumXX - sumX * sumX;
                                if (denominator == 0.0)
                                    return null;
                                return DoubleType.instance.decompose((count * sumXY - sumX * sumY) / denominator);
                            }
                            default:
                                throw new AssertionError(kind);
                        }
                    }
                };
            }
        };
    }

    /**
     * Builds the {@code percentile(value, q)} aggregate: the exact continuous percentile (PERCENTILE_CONT, with linear
     * interpolation between adjacent values) of a numeric column, where {@code q} is a quantile in {@code [0, 1]}.
     *
     * <p>This keeps every value of the group in memory and sorts on {@link Aggregate#compute}, so memory is proportional
     * to the number of rows aggregated. It is intended for downsampled time-series buckets of bounded cardinality.
     *
     * @param valueType the numeric type of the value argument
     * @param quantileType the numeric type of the quantile argument
     */
    private static NativeAggregateFunction makePercentileFunction(AbstractType<?> valueType,
                                                                  AbstractType<?> quantileType)
    {
        return new NativeAggregateFunction("percentile", DoubleType.instance, valueType, quantileType)
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    // Primitive growable buffer (avoids boxing every value into a Double).
                    private double[] values = new double[16];
                    private int size;
                    private double quantile;
                    private boolean quantileSet;

                    @Override
                    public void reset()
                    {
                        size = 0;
                        quantile = 0;
                        quantileSet = false;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        Number q = arguments.get(1);

                        if (q != null && !quantileSet)
                        {
                            double quantileValue = q.doubleValue();
                            if (quantileValue < 0 || quantileValue > 1)
                                throw invalidRequest("The quantile argument of percentile must be between 0 and 1, " +
                                                     "but was %s", quantileValue);
                            quantile = quantileValue;
                            quantileSet = true;
                        }

                        if (value != null)
                        {
                            if (size == values.length)
                                values = Arrays.copyOf(values, size * 2);
                            values[size++] = value.doubleValue();
                        }
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        if (size == 0 || !quantileSet)
                            return null;

                        Arrays.sort(values, 0, size);

                        // PERCENTILE_CONT: interpolate linearly between the values bracketing the fractional rank.
                        double rank = quantile * (size - 1);
                        int lower = (int) Math.floor(rank);
                        int upper = (int) Math.ceil(rank);
                        double result = lower == upper
                                        ? values[lower]
                                        : values[lower] + (rank - lower) * (values[upper] - values[lower]);

                        return DoubleType.instance.decompose(result);
                    }
                };
            }
        };
    }

    /**
     * Builds the {@code time_weighted_average(value, timestamp)} aggregate: the time-weighted mean of a numeric series,
     * computed as the trapezoidal integral of the value over time divided by the total time span.
     *
     * <p>Keeps the group's {@code (timestamp, value)} pairs and sorts them by timestamp on
     * {@link Aggregate#compute}, so the result is independent of row arrival order. A group with a single sample
     * returns that sample's value; an empty group returns {@code null}.
     *
     * @param valueType the numeric type of the value argument
     * @param timestampType the type of the timestamp argument ({@code timestamp} or {@code bigint})
     */
    /** Builds the {@code time_weighted_average} (average=true) / {@code integral} (average=false) factory. */
    private static FunctionFactory timeIntegralFactory(String functionName, boolean average)
    {
        return new FunctionFactory(functionName,
                                   FunctionParameter.anyType(false),
                                   FunctionParameter.fixed(CQL3Type.Native.TIMESTAMP, CQL3Type.Native.BIGINT))
        {
            @Override
            protected NativeFunction doGetOrCreateFunction(List<AbstractType<?>> argTypes, AbstractType<?> receiverType)
            {
                if (!(argTypes.get(0) instanceof NumberType))
                    throw invalidRequest("Function %s requires a numeric value argument, but found type %s",
                                         functionName, argTypes.get(0).asCQL3Type());

                return makeTimeIntegralFunction(functionName, average, argTypes.get(0), argTypes.get(1));
            }
        };
    }

    private static NativeAggregateFunction makeTimeIntegralFunction(String name,
                                                                    boolean average,
                                                                    AbstractType<?> valueType,
                                                                    AbstractType<?> timestampType)
    {
        return new NativeAggregateFunction(name, DoubleType.instance, valueType, timestampType)
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    // Parallel primitive buffers (no per-row object allocation). Rows from a partition usually arrive
                    // in timestamp order, so we track that and skip sorting in the common case.
                    private long[] times = new long[16];
                    private double[] vals = new double[16];
                    private int size;
                    private boolean sorted = true;

                    @Override
                    public void reset()
                    {
                        size = 0;
                        sorted = true;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        Number timestamp = arguments.get(1);

                        if (value == null || timestamp == null)
                            return;

                        long t = timestamp.longValue();
                        if (size > 0 && t < times[size - 1])
                            sorted = false;
                        if (size == times.length)
                        {
                            times = Arrays.copyOf(times, size * 2);
                            vals = Arrays.copyOf(vals, size * 2);
                        }
                        times[size] = t;
                        vals[size] = value.doubleValue();
                        size++;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        if (size == 0)
                            return null;
                        // With no time span there is no area: the average is the (single) value, the integral is 0.
                        if (size == 1)
                            return DoubleType.instance.decompose(average ? vals[0] : 0.0);

                        // Integrate over timestamp order. Both orders a time-series partition actually
                        // arrives in are read positionally, so neither allocates a permutation.
                        int direction = arrivalDirection(times, size, sorted);
                        int[] order = direction == ARRIVED_UNORDERED ? timestampPermutation(times, size) : null;

                        int first = orderedIndex(order, direction, size, 0);
                        int last = orderedIndex(order, direction, size, size - 1);
                        long totalMillis = times[last] - times[first];
                        if (totalMillis == 0)
                            return DoubleType.instance.decompose(average ? vals[first] : 0.0);

                        // Trapezoidal area in value-milliseconds.
                        double area = 0;
                        int prev = first;
                        for (int i = 1; i < size; i++)
                        {
                            int cur = orderedIndex(order, direction, size, i);
                            double dt = times[cur] - times[prev];
                            double meanValue = (vals[cur] + vals[prev]) / 2.0;
                            area += meanValue * dt;
                            prev = cur;
                        }

                        // time_weighted_average = area / span; integral = area expressed in value-seconds.
                        return DoubleType.instance.decompose(average ? area / totalMillis : area / 1000.0);
                    }

                };
            }
        };
    }

    /** {@link #arrivalDirection}: the samples arrived in ascending timestamp order. */
    private static final int ARRIVED_ASCENDING = 1;
    /** {@link #arrivalDirection}: the samples arrived in strictly descending timestamp order. */
    private static final int ARRIVED_DESCENDING = -1;
    /** {@link #arrivalDirection}: neither; a permutation from {@link #timestampPermutation} is required. */
    private static final int ARRIVED_UNORDERED = 0;

    /**
     * Classifies the arrival order of {@code times[0, size)} so a caller that only needs to <em>read</em> the
     * samples in timestamp order can do it positionally, with no permutation array at all.
     *
     * <p>Both orders this recognises are the ones a time-series partition actually arrives in: ascending for
     * in-order ingest and an ASC clustering, descending for a table declared
     * {@code CLUSTERING ORDER BY (timestamp DESC)}, which is the ordinary shape. Materialising an index
     * permutation for either of them costs an {@code int[size]} — 160 kB on a 40,000-row partition — plus a fill
     * pass, and then makes every read in the aggregation loop a dependent load ({@code times[order[i]]}) where a
     * flat {@code times[i]} would do. Only the genuinely-unordered case needs the array.
     *
     * <p>Descending must be <em>strict</em>: reading a run backwards is only stable when no two timestamps tie,
     * and with a tie the stable merge in {@link #timestampPermutation} is required to keep arrival order — which
     * is what lets a re-written sample at the same timestamp win.
     *
     * @param sorted the caller's own record of whether every sample arrived at or after its predecessor, kept
     *               during {@code addInput} so the ascending case costs nothing to detect here
     * @return {@link #ARRIVED_ASCENDING}, {@link #ARRIVED_DESCENDING} or {@link #ARRIVED_UNORDERED}
     */
    private static int arrivalDirection(long[] times, int size, boolean sorted)
    {
        if (sorted || size < 2)
            return ARRIVED_ASCENDING;

        for (int i = 1; i < size; i++)
            if (times[i - 1] <= times[i])
                return ARRIVED_UNORDERED;
        return ARRIVED_DESCENDING;
    }

    /**
     * The array index holding the {@code i}-th sample in timestamp order.
     *
     * @param permutation {@code null} when {@code direction} already describes the order, else the permutation
     *                    from {@link #timestampPermutation}
     * @param direction   {@link #ARRIVED_ASCENDING} or {@link #ARRIVED_DESCENDING}; ignored when
     *                    {@code permutation} is non-null
     */
    private static int orderedIndex(int[] permutation, int direction, int size, int i)
    {
        if (permutation != null)
            return permutation[i];
        return direction == ARRIVED_ASCENDING ? i : size - 1 - i;
    }

    /**
     * Returns an index permutation of {@code [0, size)} ordering {@code times} ascending, stably: indices whose
     * timestamps tie keep their arrival order, which is what lets a re-written sample at the same timestamp win.
     *
     * <p>Only for input {@link #arrivalDirection} classified as {@link #ARRIVED_UNORDERED}; the two ordered cases
     * are read positionally and never come here. A bottom-up merge sort of the indices, O(n log n), stable, one
     * {@code int[]} of scratch and no autoboxing.
     *
     * <p>Not an insertion sort, which is what this used to be: insertion sort is O(n) on an already-sorted input
     * but O(n²) on a reversed one, and a {@code CLUSTERING ORDER BY (timestamp DESC)} table delivers rows in
     * exactly that reversed order. On a 100,000-row partition that was ~5e9 comparisons and measured at 14.4 s,
     * against ~0.5 s for every other aggregate over the same scan.
     */
    private static int[] timestampPermutation(long[] times, int size)
    {
        int[] order = new int[size];
        for (int i = 0; i < size; i++)
            order[i] = i;

        int[] scratch = new int[size];
        for (int width = 1; width < size; width <<= 1)
        {
            for (int lo = 0; lo < size; lo += width << 1)
            {
                int mid = Math.min(lo + width, size);
                int hi = Math.min(lo + (width << 1), size);
                int i = lo, j = mid, k = lo;
                // '<=' keeps the left run first on a tie, which is what makes the sort stable.
                while (i < mid && j < hi)
                    scratch[k++] = times[order[i]] <= times[order[j]] ? order[i++] : order[j++];
                while (i < mid)
                    scratch[k++] = order[i++];
                while (j < hi)
                    scratch[k++] = order[j++];
            }
            int[] swap = order;
            order = scratch;
            scratch = swap;
        }
        return order;
    }

    /**
     * Builds the {@code counter_delta}/{@code counter_rate} aggregate over a numeric {@code (value, timestamp)} series.
     * The total increase of a monotonically increasing counter is summed; when the value drops between consecutive
     * samples (a counter reset), the post-reset value is taken as that step's increase (Prometheus-style). Order-
     * independent: samples are buffered and ordered by timestamp at finalization.
     *
     * @param name the function name
     * @param perSecond {@code true} for {@code counter_rate} (increase per second), {@code false} for {@code counter_delta}
     * @param valueType the numeric type of the value argument
     * @param timestampType the type of the timestamp argument ({@code timestamp} or {@code bigint})
     */
    private static NativeAggregateFunction makeCounterFunction(String name,
                                                               boolean perSecond,
                                                               AbstractType<?> valueType,
                                                               AbstractType<?> timestampType)
    {
        return new NativeAggregateFunction(name, DoubleType.instance, valueType, timestampType)
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private long[] times = new long[16];
                    private double[] vals = new double[16];
                    private int size;
                    private boolean sorted = true;

                    @Override
                    public void reset()
                    {
                        size = 0;
                        sorted = true;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        Number timestamp = arguments.get(1);

                        if (value == null || timestamp == null)
                            return;

                        long t = timestamp.longValue();
                        if (size > 0 && t < times[size - 1])
                            sorted = false;
                        if (size == times.length)
                        {
                            times = Arrays.copyOf(times, size * 2);
                            vals = Arrays.copyOf(vals, size * 2);
                        }
                        times[size] = t;
                        vals[size] = value.doubleValue();
                        size++;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        if (size == 0)
                            return null;
                        if (size == 1)
                            // A single sample has no increase and no time span.
                            return perSecond ? null : DoubleType.instance.decompose(0.0);

                        int direction = arrivalDirection(times, size, sorted);
                        int[] order = direction == ARRIVED_UNORDERED ? timestampPermutation(times, size) : null;

                        double increase = 0;
                        double prev = vals[orderedIndex(order, direction, size, 0)];
                        for (int i = 1; i < size; i++)
                        {
                            double cur = vals[orderedIndex(order, direction, size, i)];
                            // On a reset (value dropped) the counter is assumed to have restarted from 0.
                            increase += cur >= prev ? cur - prev : cur;
                            prev = cur;
                        }

                        if (!perSecond)
                            return DoubleType.instance.decompose(increase);

                        long spanMillis = times[orderedIndex(order, direction, size, size - 1)]
                                          - times[orderedIndex(order, direction, size, 0)];
                        if (spanMillis == 0)
                            return null;
                        return DoubleType.instance.decompose(increase / (spanMillis / 1000.0));
                    }
                };
            }
        };
    }

    /**
     * Builds the {@code variance}/{@code stddev} aggregate (sample statistics, dividing by {@code n - 1}).
     *
     * @param name the function name
     * @param sqrt {@code true} for {@code stddev} (square root of the variance), {@code false} for {@code variance}
     * @param valueType the numeric type of the argument
     */
    private static NativeAggregateFunction makeVarianceFunction(String name, boolean sqrt, AbstractType<?> valueType)
    {
        return new NativeAggregateFunction(name, DoubleType.instance, valueType)
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private long count;
                    private double sum;
                    private double sumOfSquares;

                    @Override
                    public void reset()
                    {
                        count = 0;
                        sum = 0;
                        sumOfSquares = 0;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        if (value == null)
                            return;

                        double v = value.doubleValue();
                        sum += v;
                        sumOfSquares += v * v;
                        count++;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        // Sample variance is undefined for fewer than two values.
                        if (count < 2)
                            return null;

                        double variance = (sumOfSquares - (sum * sum) / count) / (count - 1);
                        // Guard against tiny negative values from floating-point cancellation.
                        if (variance < 0)
                            variance = 0;

                        return DoubleType.instance.decompose(sqrt ? Math.sqrt(variance) : variance);
                    }
                };
            }
        };
    }

    /**
     * Builds the {@code histogram(value, min, max, nbuckets)} aggregate: a list of {@code nbuckets + 2} counts — the
     * first is the underflow count (values {@code < min}), the last is the overflow count (values {@code >= max}), and
     * the middle {@code nbuckets} entries are equal-width buckets over {@code [min, max)}. Mirrors the histogram
     * function found in other time-series databases.
     *
     * <p>{@code min}, {@code max} and {@code nbuckets} are expected to be constants; they are read from the first row.
     *
     * @param argTypes the [value, min, max, nbuckets] argument types
     */
    private static NativeAggregateFunction makeHistogramFunction(List<AbstractType<?>> argTypes)
    {
        ListType<Long> histogramType = ListType.getInstance(LongType.instance, false);
        return new NativeAggregateFunction("histogram",
                                           histogramType,
                                           argTypes.get(0), argTypes.get(1), argTypes.get(2), argTypes.get(3))
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private long[] counts;
                    private double min;
                    private double max;
                    private int nbuckets;
                    private boolean boundsSet;

                    @Override
                    public void reset()
                    {
                        counts = null;
                        boundsSet = false;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number value = arguments.get(0);
                        Number minArg = arguments.get(1);
                        Number maxArg = arguments.get(2);
                        Number nbucketsArg = arguments.get(3);

                        if (!boundsSet)
                        {
                            if (minArg == null || maxArg == null || nbucketsArg == null)
                                return;

                            min = minArg.doubleValue();
                            max = maxArg.doubleValue();
                            nbuckets = nbucketsArg.intValue();

                            if (nbuckets <= 0)
                                throw invalidRequest("Function histogram requires nbuckets > 0, but was %s", nbuckets);
                            if (max <= min)
                                throw invalidRequest("Function histogram requires max > min, but was min=%s max=%s",
                                                     min, max);

                            counts = new long[nbuckets + 2];
                            boundsSet = true;
                        }

                        if (value == null)
                            return;

                        double v = value.doubleValue();
                        int index;
                        if (v < min)
                            index = 0;
                        else if (v >= max)
                            index = nbuckets + 1;
                        else
                            index = 1 + (int) ((v - min) / (max - min) * nbuckets);

                        counts[index]++;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        if (!boundsSet)
                            return null;

                        List<Long> result = new ArrayList<>(counts.length);
                        for (long c : counts)
                            result.add(c);

                        return histogramType.decompose(result);
                    }
                };
            }
        };
    }

    /**
     * Builds the {@code corr}/{@code covar_pop}/{@code covar_samp} aggregate over two numeric columns {@code (y, x)}.
     * Uses constant-memory running sums (n, &Sigma;x, &Sigma;y, &Sigma;xy, &Sigma;x&sup2;, &Sigma;y&sup2;), so the
     * result is independent of row order. Returns {@code null} when undefined (too few rows, or zero variance for
     * {@code corr}).
     *
     * @param name the function name
     * @param kind the statistic to compute
     * @param yType the numeric type of the first argument
     * @param xType the numeric type of the second argument
     */
    private static NativeAggregateFunction makeBivariateFunction(String name,
                                                                 BivariateStat kind,
                                                                 AbstractType<?> yType,
                                                                 AbstractType<?> xType)
    {
        return new NativeAggregateFunction(name, DoubleType.instance, yType, xType)
        {
            @Override
            public Aggregate newAggregate()
            {
                return new Aggregate()
                {
                    private long count;
                    private double sumX;
                    private double sumY;
                    private double sumXY;
                    private double sumXX;
                    private double sumYY;

                    @Override
                    public void reset()
                    {
                        count = 0;
                        sumX = sumY = sumXY = sumXX = sumYY = 0;
                    }

                    @Override
                    public void addInput(Arguments arguments)
                    {
                        Number y = arguments.get(0);
                        Number x = arguments.get(1);
                        if (y == null || x == null)
                            return;

                        double yv = y.doubleValue();
                        double xv = x.doubleValue();
                        sumX += xv;
                        sumY += yv;
                        sumXY += xv * yv;
                        sumXX += xv * xv;
                        sumYY += yv * yv;
                        count++;
                    }

                    @Override
                    public ByteBuffer compute(FunctionContext context)
                    {
                        switch (kind)
                        {
                            case COVAR_POP:
                                if (count < 1)
                                    return null;
                                return DoubleType.instance.decompose((sumXY - sumX * sumY / count) / count);
                            case COVAR_SAMP:
                                if (count < 2)
                                    return null;
                                return DoubleType.instance.decompose((sumXY - sumX * sumY / count) / (count - 1));
                            case CORR:
                            {
                                if (count < 2)
                                    return null;
                                double denominator = Math.sqrt((count * sumXX - sumX * sumX)
                                                               * (count * sumYY - sumY * sumY));
                                if (denominator == 0)
                                    return null;
                                return DoubleType.instance.decompose((count * sumXY - sumX * sumY) / denominator);
                            }
                            case REGR_SLOPE:
                            {
                                if (count < 2)
                                    return null;
                                double varX = count * sumXX - sumX * sumX;
                                if (varX == 0)
                                    return null;
                                return DoubleType.instance.decompose((count * sumXY - sumX * sumY) / varX);
                            }
                            case REGR_INTERCEPT:
                            {
                                if (count < 2)
                                    return null;
                                double varX = count * sumXX - sumX * sumX;
                                if (varX == 0)
                                    return null;
                                double slope = (count * sumXY - sumX * sumY) / varX;
                                return DoubleType.instance.decompose((sumY - slope * sumX) / count);
                            }
                            case REGR_R2:
                            {
                                if (count < 2)
                                    return null;
                                double denom = (count * sumXX - sumX * sumX) * (count * sumYY - sumY * sumY);
                                if (denom == 0)
                                    return null;
                                double cov = count * sumXY - sumX * sumY;
                                return DoubleType.instance.decompose(cov * cov / denom);
                            }
                            default:
                                throw new AssertionError(kind);
                        }
                    }
                };
            }
        };
    }

    private TimeSeriesFcts()
    {
    }
}
