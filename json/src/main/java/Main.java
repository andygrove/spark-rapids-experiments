import ai.rapids.cudf.*;

import java.io.File;

public class Main {
  public static void main(String args[]) {

    Schema schema = Schema.builder()
            .column(DType.INT64, "Zipcode")
            .column(DType.STRING, "ZipcodeType")
            .column(DType.STRING, "City")
            .column(DType.STRING, "State")
            .build();

    try (ColumnVector v = ColumnVector.fromStrings(
            "{ state: \"CA\" }",
            "{ state: [{ \"name\": \"CA\" }]")) {

      Table t = Table.readJSON(schema, new File("test.json"));

      TableDebug.get().debug("table", t);

    }
    System.out.println("done");
  }
}

