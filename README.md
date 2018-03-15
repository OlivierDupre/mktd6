# Kafka Streams Hands On - Monkonomy

You are a trader, trading coins for banana shares.

Coins can buy banana shares.

Tweets can manipulate the share prices.

Banana shares can feed monkeys. 

You task is to make monkeys happy.

## Katas

Start with some exercises to familiarize yourself with the Kafka Streams API.

The exercises are located in `monkonomy-kafkastreams-katas/src/test/java/mktd6`.

They take the form of unit test that you have to complete.

Do them in order (starting at Chapter0...), read the comments.

## General setup

### Maven settings

If you want to download dependencies faster, add this to your `.m2/settings.xml`, 
replacing the "`host`" by a value given at the event beginning:

```xml
<settings>
  ...
  <mirrors>
    <mirror>
      <id>central</id>
      <name>dep-hosting</name>
      <url>http://host:7000/maven</url>
      <mirrorOf>*</mirrorOf>
    </mirror>
  </mirrors>
  ...
</settings>
```

### Launch a local monkonomy server (needs docker and docker-compose)

You may want to inspect kafka topics while tinkering.

In order to do so:

```bash
$ ./run-kafka.sh
```

This will launch the `docker-compose.yml`, providing your local network IP.

Then build and run the monkonomy server:

```bash
$ ./build.sh install
$ ./run.sh server
```

Once the server runs, you may launch your trading app:

```bash
$ ./run.sh trader
```
