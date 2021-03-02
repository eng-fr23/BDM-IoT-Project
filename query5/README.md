### Building the query5 project

The application can be started through the spark-submit utility in a
premade spark container, interfacing with a mongodb container in a docker network.

The mongo container is exposed on the 27017 tcp port.

Run with:

```bash
cd query5
docker-compose up --build -d
```