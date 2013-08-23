# Heller

[![Build Status](https://travis-ci.org/mthssdrbrg/heller.png?branch=master)](https://travis-ci.org/mthssdrbrg/heller)

Heller is a thin JRuby wrapper around the Kafka Producer and (Simple)Consumer APIs, much like [Mikka](https://github.com/iconara/mikka) is a JRuby wrapper around Akka.

The goal of Heller is to make the Producer and Consumer APIs of Kafka more
Rubyesque, and useful as building blocks for creating more advanced producer and
consumer implementations.

## Producer API

```Heller::Producer``` is an extremely simple wrapper class around
```Kafka::Producer::Producer``` and provides some convenience in configuring the
producer with more Rubyesque names for configuration parameters.

All configuration parameters are supported and can be used in the following way:

```ruby
producer = Heller::Producer.new('localhost:9092,localhost:9093' {
  :type => :async,
  :serializer => 'kafka.serializer.StringEncoder',
  :key_serializer => 'kafka.serializer.DefaultEncoder',
  :partitioner => 'kafka.producer.DefaultPartitioner',
  :compression => :gzip,
  :num_retries => 5,
  :retry_backoff => 1500,
  :metadata_refresh_interval => 5000,
  :batch_size => 2000,
  :client_id => 'spec-client',
  :request_timeout => 10000,
  :buffer_limit => 100 * 100,
  :buffer_timeout => 1000 * 100,
  :enqueue_timeout => 1000,
  :socket_buffer => 1024 * 1000,
  :ack => -1
})
```

Check the official [Kafka docs](http://kafka.apache.org/documentation.html#producerapi) for possible values for each parameter.

To send messages you creates instances of Heller::Message and feed them to the
```#push``` method of the producer:

```ruby
messages = 3.times.map { Heller::Message.new('test', 'my message!') }
producer.push(messages)
```

Want to partition messages based on some key? Sure, no problem:

```ruby
messages = [0, 1, 2].map { |key| Heller::Message.new('test', "my message using #{key} as key!", key.to_s) }
producer.push(messages)
```

## Status

The project is currently under development, and I wouldn't really recommend it
to be used in any form of production environment. There is still quite some work
that needs to be done for both the Producer and Consumer APIs.

It's getting there, though I'm mostly doing this during my spare time, which is
sparse at times.