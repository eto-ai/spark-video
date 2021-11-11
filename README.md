# spark-video
## For User
### Databricks Runtime
Put these two lines in the init script:
```
sudo wget -O /databricks/jars/ffmpeg-4.4-1.5.6-linux-x86_64.jar https://repo1.maven.org/maven2/org/bytedeco/ffmpeg/4.4-1.5.6/ffmpeg-4.4-1.5.6-linux-x86_64.jar
sudo wget -O /databricks/jars/javacpp-1.5.6-linux-x86_64.jar https://repo1.maven.org/maven2/org/bytedeco/javacpp/1.5.6/javacpp-1.5.6-linux-x86_64.jar
```

Upload the assembly jar to the Databricks Cluster Libraries:
```
bin/mill video.assembly # Here is the jar: out/video/assembly/dest/out.jar
```

## For Developer
### Cheatsheet
```
bin/mill video.test
bin/mill video.publishLocal

bin/mill mill.scalalib.scalafmt.ScalafmtModule/checkFormatAll __.sources
bin/mill mill.scalalib.scalafmt.ScalafmtModule/reformatAll __.sources
```
