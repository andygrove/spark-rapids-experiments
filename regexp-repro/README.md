
Attempt at reproducing https://github.com/NVIDIA/spark-rapids/issues/6431

Edit `Main.java` and experiment with the hard-coded values, then run:

```bash
mvn compile exec:java -Dexec.mainClass="Main"
```