### Building the query4 project

Build the project with maven clean install, use the generated jar with hadoop or use the compose 
configuration present with the prebuilt jar:

```bash
cd dist
docker-compose up -d
docker exec -it dist_master_1 bash
./hadoop_mr.sh
```