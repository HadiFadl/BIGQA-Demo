package mitri;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.function.BiFunction;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

@UdafDescription(name = "not_null_percentage",
    description = "Returns the not null factor ( 0 -1) of the column computed as the count of "
                 + "non nulls divided by the  count of total. Applicable only to numeric types"
                 + "supports distrubuted system of KSQLDB, supports aggregation and time windowing"
                 + " the value -1 is also considered a null value",
    author = "Mitri Haber"
)
public final class not_null_percentage {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";
  private static final Schema STRUCT_LONG = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_INT64_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private static final Schema STRUCT_INT = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_INT32_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private static final Schema STRUCT_DOUBLE = SchemaBuilder.struct().optional()
      .field(SUM, Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field(COUNT, Schema.OPTIONAL_INT64_SCHEMA)
      .build();


  private not_null_percentage() {
  }


  @UdafFactory(description = "Compute not null percentage of column with type Long.",
      aggregateSchema = "STRUCT<SUM bigint, COUNT bigint>")
  public static TableUdaf<Long, Struct, Double> nnLong() {
    return getAverageImplementation(
        0L,
        STRUCT_LONG,
        (sum, newValue) -> newValue != -1 ? sum.getInt64(SUM) + 1 : sum.getInt64(SUM),
        (sum, count) -> sum.getInt64(SUM) / count,
        (sum1, sum2) -> sum1.getInt64(SUM) + sum2.getInt64(SUM),
        (sum, valueToUndo) -> sum.getInt64(SUM) - valueToUndo);
  }

  @UdafFactory(description = "Compute not null percentage of column with type Integer.",
      aggregateSchema = "STRUCT<SUM integer, COUNT bigint>")
  public static TableUdaf<Integer, Struct, Double> nnInt() {

    return getAverageImplementation(
        0,
        STRUCT_INT,
        (sum, newValue) -> newValue != -1 ? sum.getInt32(SUM) + 1 : sum.getInt32(SUM),
        (sum, count) -> sum.getInt32(SUM) / count,
        (sum1, sum2) -> sum1.getInt32(SUM) + sum2.getInt32(SUM),
        (sum, valueToUndo) -> sum.getInt32(SUM) - valueToUndo);
  }

  @UdafFactory(description = "Compute not null percentage of column with type Double.",
      aggregateSchema = "STRUCT<SUM double, COUNT bigint>")
  public static TableUdaf<Double, Struct, Double> nnDouble() {

    return getAverageImplementation(
        0.0,
        STRUCT_DOUBLE,
        (sum, newValue) -> newValue != -1 ? sum.getFloat64(SUM) + 1 : sum.getFloat64(SUM),
        (sum, count) -> sum.getFloat64(SUM) / count,
        (sum1, sum2) -> sum1.getFloat64(SUM) + sum2.getFloat64(SUM),
        (sum, valueToUndo) -> sum.getFloat64(SUM) - valueToUndo);
  }


  private static <I> TableUdaf<I, Struct, Double> getAverageImplementation(
      final I initialValue,
      final Schema structSchema,
      final BiFunction<Struct, I, I> adder,
      final BiFunction<Struct, Double, Double> mapper,
      final BiFunction<Struct, Struct, I> merger,
      final BiFunction<Struct, I, I> subtracter) {

    return new TableUdaf<I, Struct, Double>() {

      @Override
      public Struct initialize() {
        return new Struct(structSchema).put(SUM, initialValue).put(COUNT, 0L);
      }

      @Override
      public Struct aggregate(final I newValue,
                              final Struct aggregate) {

        if (newValue == null || newValue.equals(-1) ) {
          return aggregate;
        }
        return new Struct(structSchema)
            .put(SUM, adder.apply(aggregate, newValue))
            .put(COUNT, aggregate.getInt64(COUNT) + 1);

      }

      @Override
      public Double map(final Struct aggregate) {
        final long count = aggregate.getInt64(COUNT);
        if (count == 0) {
          return 0.0;
        }
        return mapper.apply(aggregate,((double)count));
      }

      @Override
      public Struct merge(final Struct agg1,
                          final Struct agg2) {

        return new Struct(structSchema)
            .put(SUM, merger.apply(agg1, agg2))
            .put(COUNT, agg1.getInt64(COUNT) + agg2.getInt64(COUNT));
      }

      @Override
      public Struct undo(final I valueToUndo,
                         final Struct aggregate) {
        if (valueToUndo == null) {
          return aggregate;
        }
        return new Struct(structSchema)
            .put(SUM, subtracter.apply(aggregate, valueToUndo))
            .put(COUNT, aggregate.getInt64(COUNT) - 1);
      }
    };
  }
}
