import ai.rapids.cudf.*;

public class Main {
  public static void main(String args[]) {
    try (ColumnVector v = ColumnVector.fromStrings("one\ntwo", "three\n\n")) {
      String pattern = "[^\n\r\u0085\u2028\u2029]*(\r|\u0085|\u2028|\u2029|\r\n)?$";
      String repl = "scala${1}";
      RegexProgram prog = new RegexProgram(pattern);
      v.stringReplaceWithBackrefs(prog, repl);
    }
  }
}

