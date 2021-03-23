### Building the query6 project

Like for the query5 application, the app is started through the spark-submit utility in a
premade spark container, interfacing with a mongodb container in a docker network.

The structure of the application is the following:

- A sender service acts as the streaming server; it opens up the specified dataset and 
  streams it on the specified port, that can be changed in the configuration file.
- A spark service acts as the spark master and acquires data through a stream coming from the 
sender service, after connecting to it through the spark streaming API.
- A mongo instance in a container acts as the database where to save the results of the computations.

The mongo container is exposed on the 27017 tcp port.

Run with:

```bash
cd query6
docker-compose up -d
```