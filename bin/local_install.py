import os
import subprocess
import platform

spark_home = os.environ.get("SPARK_HOME")

if platform.system() == 'Darwin':
    classifier = "macosx-x86_64"
else:
    classifier = "linux-x86_64"

version = "0.0.4"

ffmpeg = f"https://repo1.maven.org/maven2/org/bytedeco/ffmpeg/4.4-1.5.6/ffmpeg-4.4-1.5.6-{classifier}.jar"
javacpp = f"https://repo1.maven.org/maven2/org/bytedeco/javacpp/1.5.6/javacpp-1.5.6-{classifier}.jar"
spark_video = f"https://github.com/eto-ai/spark-video/releases/download/v{version}/spark-video-assembly_2.12-{version}.jar"

subprocess.run(["wget", "-O", f"{spark_home}/jars/ffmpeg-{classifier}.jar", ffmpeg])
subprocess.run(["wget", "-O", f"{spark_home}/jars/javacpp-{classifier}.jar", javacpp])
subprocess.run(["wget", "-O", f"{spark_home}/jars/spark-video.jar", spark_video])
