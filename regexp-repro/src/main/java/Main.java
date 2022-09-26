import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Table;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Main {

  public static void main(String args[]) throws InterruptedException {
    doStabilityTest(166_666_667, 2, 10);
  }

  static void doStabilityTest(int n, int numThreads, int maxAttempt) throws InterruptedException {
    // This test aims to reproduce the following Spark query, which produces
    // inconsistent results between runs:
    //
    // spark.range(1000000000L)
    // .selectExpr("CAST(id as STRING) as str_id")
    //   .filter("regexp_like(str_id, '(.|\n)*1(.|\n)0(.|\n)*')")
    //   .count()


    String csvResults[] = new String[maxAttempt];

    for (int attempt = 0; attempt< maxAttempt; attempt++) {
      System.err.println("attempt " + attempt + " of " + maxAttempt);

      Thread threads[] = new Thread[numThreads];
      long result[] = new long[numThreads];
      for (int j = 0; j < numThreads; j++) {
        int threadNo = j;
        threads[j] = new Thread(() -> {

          int chunk_size = n/numThreads;
          long array[] = new long[chunk_size];
          for (int i = 0; i < chunk_size; i++) {
            array[i] = chunk_size * threadNo + i;
          }

          try (ColumnVector longs = ColumnVector.fromLongs(array);
               ColumnVector castTo = longs.castTo(DType.STRING);
               ColumnVector matchesRe = castTo.matchesRe("(.|\\n)*1(.|\\n)0(.|\\n)*");
               Table t = new Table(longs);
               Table t2 = t.filter(matchesRe)) {
            result[threadNo] = t2.getRowCount();
            System.err.println("thread " + threadNo + " count: " + t2.getRowCount());
          }

        });
        threads[j].start();
      }

      for (Thread t : threads) {
        t.join();
      }

      csvResults[attempt] = Arrays.stream(result)
              .mapToObj(String::valueOf)
              .collect(Collectors.joining(","));
    }

    // check that the results from each run are the same
    for (int attempt = 0; attempt< maxAttempt; attempt++) {
      System.out.println(csvResults[attempt]);
      if (!csvResults[0].equals(csvResults[attempt])) {
        throw new IllegalStateException();
      }
    }

  }

}

