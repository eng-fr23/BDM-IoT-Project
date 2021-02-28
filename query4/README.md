### Building the query4 project

On Intellij, Project Structure -> Artifacts -> JAR

then include all modules, specify main/java/resources as the manifest directory, 
and build via Build -> Build Artifacts -> Build

Move the jar into dist, download the csv dataset into dist, run the docker-compose,
ssh into the master, and run the hadoop_mr.sh script.