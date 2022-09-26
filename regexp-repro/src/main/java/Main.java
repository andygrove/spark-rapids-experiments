import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.DType;
import ai.rapids.cudf.Table;

import java.util.Arrays;
import java.util.stream.Collectors;

public class Main {

  public static void main(String args[]) throws InterruptedException {
    doStabilityTest(2 * 166_666_667, 2, 10);
  }

  static void doStabilityTest(int n, int numThreads, int maxAttempt) throws InterruptedException {
    // This test aims to reproduce the following Spark query, which produces
    // inconsistent results between runs:
    //
    // spark.range(1000000000L)
    // .selectExpr("CAST(id as STRING) as str_id")
    //   .filter("regexp_like(str_id, '(.|\n)*1(.|\n)0(.|\n)*')")
    //   .count()

    // create the input data once in host memory
    long[][] array = new long[numThreads][];
    for (int threadNo=0; threadNo<numThreads; threadNo++) {
      int chunk_size = n / numThreads;
      array[threadNo] = new long[chunk_size];
      long[] x = array[threadNo];
      for (int i = 0; i < chunk_size; i++) {
        x[i] = (long) chunk_size * threadNo + i;
      }
    }

    String csvResults[] = new String[maxAttempt];
    for (int attempt = 0; attempt< maxAttempt; attempt++) {
      System.err.println("attempt " + attempt + " of " + maxAttempt);

      Thread threads[] = new Thread[numThreads];
      long result[] = new long[numThreads];
      for (int j = 0; j < numThreads; j++) {
        int threadNo = j;
        threads[j] = new Thread(() -> {

          // cast longs to string then release the longs vector
          ColumnVector castTo;
          try (ColumnVector longs = ColumnVector.fromLongs(array[threadNo])) {
               castTo = longs.castTo(DType.STRING);
          }

          // matchesRe and filter
          try (ColumnVector matchesRe = castTo.matchesRe("(.|\\n)*1(.|\\n)0(.|\\n)*");
               Table t = new Table(castTo);
               Table t2 = t.filter(matchesRe)) {
            result[threadNo] = t2.getRowCount();
            System.err.println("thread " + threadNo + " count: " + t2.getRowCount());
          }

        });

      }

      // start the threads
      for (int j = 0; j < numThreads; j++) {
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

