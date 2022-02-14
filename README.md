# spark-video
## For User
### Local Development
Just copy and paste the python code from [bin/local_install.py](bin/local_install.py) to the pyspark REPL.

### Google Colab
See [notebooks/GoogleColabDemo.ipynb](notebooks/GoogleColabDemo.ipynb)

### Production: Databricks Runtime
Put these three lines in the init script:
```
sudo wget -O /databricks/jars/ffmpeg-4.4-1.5.6-linux-x86_64.jar https://repo1.maven.org/maven2/org/bytedeco/ffmpeg/4.4-1.5.6/ffmpeg-4.4-1.5.6-linux-x86_64.jar
sudo wget -O /databricks/jars/javacpp-1.5.6-linux-x86_64.jar https://repo1.maven.org/maven2/org/bytedeco/javacpp/1.5.6/javacpp-1.5.6-linux-x86_64.jar
sudo wget -O /databricks/jars/spark-video-assembly-0.0.4.jar https://github.com/eto-ai/spark-video/releases/download/v0.0.4/spark-video-assembly_2.12-0.0.4.jar
```

## For Developer
### Cheatsheet
```
bin/mill 'video[2.12].test'
bin/mill 'video[2.12].test.testOnly' '**.MLImageTest.scala'
bin/mill 'video[2.12].publishLocal'
bin/mill 'video[2.12].assembly'

bin/mill mill.scalalib.scalafmt.ScalafmtModule/checkFormatAll __.sources
bin/mill mill.scalalib.scalafmt.ScalafmtModule/reformatAll __.sources
```
