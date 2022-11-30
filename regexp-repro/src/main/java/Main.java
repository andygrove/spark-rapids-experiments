import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;

import java.util.regex.Pattern;

public class Main {

  public static void main(String args[]) {

    String[] inputs = {
            "hello\n",
            "hell\no\n",
            "hell\no\n\n",
            "goodbye",
    };

    String pattern = "o$";

    System.out.println("Pattern = " + pattern);

    boolean[] cpu = cpuContains(pattern, inputs);
    boolean[] gpu = gpuContains(pattern, inputs);

    for (int i=0; i<inputs.length; i++) {
      System.out.println(inputs[i] + " -> cpu=" + cpu[i] + "; gpu=" + gpu[i]);
    }
  }

  private static boolean[] cpuContains(String pattern, String[] inputs) {
    Pattern p = Pattern.compile(pattern);
    boolean ret[] = new boolean[inputs.length];
    for (int i=0; i<ret.length; i++) {
      ret[i] = p.matcher(inputs[i]).find(0);
    }
    return ret;
  }

  private static boolean[] gpuContains(String pattern, String[] inputs) {
    ColumnVector cv = ColumnVector.fromStrings(inputs);
    ColumnVector cv2 = cv.containsRe(pattern);
    HostColumnVector hostColumnVector = cv2.copyToHost();
    boolean ret[] = new boolean[inputs.length];
    for (int i=0; i<ret.length; i++) {
      ret[i] = hostColumnVector.getBoolean(i);
    }
    return ret;
  }
}

