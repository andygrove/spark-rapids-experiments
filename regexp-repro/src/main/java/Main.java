import ai.rapids.cudf.ColumnVector;
import ai.rapids.cudf.HostColumnVector;
import ai.rapids.cudf.Scalar;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class Main {

  /* JAVA DEFS

  https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html

  Line terminators

    A line terminator is a one- or two-character sequence that marks the end of a
    line of the input character sequence. The following are recognized as line terminators:

    A newline (line feed) character ('\n'),
    A carriage-return character followed immediately by a newline character ("\r\n"),
    A standalone carriage-return character ('\r'),
    A next-line character ('\u0085'),
    A line-separator character ('\u2028'), or
    A paragraph-separator character ('\u2029).

  $ 	The end of a line
  \Z 	The end of the input but for the final terminator, if any
  \z 	The end of the input

   */

  public static void main(String args[]) {
    lineAnchorsFind();
  }

  /**
   * - line anchor $ - find *** FAILED ***
   - line anchor $ - find *** FAILED ***
   javaPattern[0]=a$, cudfPattern=a(?:[\n\r\u0085\u2028\u2029]|\r\n)?$, input='a\u0085\n', cpu=false, gpu=true (RegularExpressionTranspilerSuite.scala:845)

   * */
  public static void lineAnchorsFind() {

    String lineTerminators[] = {
            "\r",
            "\n",
            "\r\n",
            "\u0085",
            "\u2028",
            "\u2029"
    };

    List<String> inputs = new ArrayList<>();
    for (String lt: lineTerminators) {
      String input = "TEST" + lt;
      inputs.add(input);
      for (String lt2: lineTerminators) {
        String input2 = input + lt2;
        inputs.add(input2);
        for (String lt3: lineTerminators) {
          String input3 = input2 + lt3;
          inputs.add(input3);
        }
      }
    }

    String gpuPattern = "TEST$";
//    String gpuPattern = "TEST(\u0085|\u2028|\u2029\r\n|\r){0,1}[^\u0085\u2028\u2029\r\n]$";
    String cpuPattern = "TEST$";

    String data[] = new String[inputs.size()];
    inputs.toArray(data);
    compareCpuGpuContains(cpuPattern, gpuPattern, data);
  }

  public static void compareCpuGpuContains(String cpuPattern, String gpuPattern, String[] inputs) {
    System.out.println("Patterns: cpu=" + cpuPattern + "; gpu=" + gpuPattern);

    boolean[] cpu = cpuContains(cpuPattern, inputs);
    boolean[] gpu = gpuContains(gpuPattern, inputs);

    boolean ok = true;
    for (int i=0; i<inputs.length; i++) {
      if (cpu[i] != gpu[i]) {
        System.out.print("inputs[" + i + "] " + toReadableString(inputs[i]) + " -> cpu=" + cpu[i] + "; gpu=" + gpu[i]);
        System.out.println(" ** MISMATCH ** ");
        ok = false;
      } else {
//        System.out.println();
      }
    }

    if (!ok) {
      throw new RuntimeException("mismatch");
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
//    cv = cv.rstrip();
//    cv = cv.stringReplace(Scalar.fromString("\u2028"), Scalar.fromString("\n"));
//    cv = cv.stringReplace(Scalar.fromString("\u2029"), Scalar.fromString("\n"));
//    cv = cv.stringReplace(Scalar.fromString("\r\n"), Scalar.fromString("\n"));
//    cv = cv.stringReplace(Scalar.fromString("\r"), Scalar.fromString("\n"));
    ColumnVector cv2 = cv.containsRe(pattern);
    HostColumnVector hostColumnVector = cv2.copyToHost();
    boolean ret[] = new boolean[inputs.length];
    for (int i=0; i<ret.length; i++) {
      ret[i] = hostColumnVector.getBoolean(i);
    }
    return ret;
  }

  private static String toReadableString(String x) {
    String ret = "";
    for (int i=0; i<x.length(); i++) {
      char ch = x.charAt(i);
      String ch2 = String.valueOf(ch);
      switch (ch) {
        case '\r':
          ch2 = "\\r";
          break;
        case '\n':
          ch2 = "\\n";
          break;
        case '\t':
          ch2 = "\\t";
          break;
        case '\f':
          ch2 = "\\f";
          break;
        case '\u0000':
          ch2 = "\\u0000";
          break;
        case '\u000b':
          ch2 = "\\u000b";
          break;
        case '\u0085':
          ch2 = "\\u0085";
          break;
        case '\u2028':
          ch2 = "\\u2028";
          break;
        case '\u2029':
          ch2 = "\\u2029";
          break;
      }
      ret += ch2;
    }
    return ret;
  }
}

