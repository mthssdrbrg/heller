# Heller

[![Build Status](https://travis-ci.org/mthssdrbrg/heller.png?branch=master)](https://travis-ci.org/mthssdrbrg/heller)
[![Coverage Status](https://coveralls.io/repos/mthssdrbrg/heller/badge.png?branch=master)](https://coveralls.io/r/mthssdrbrg/heller?branch=master)

Heller is a JRuby wrapper around the Kafka Producer and (Simple)Consumer
APIs, much like [Mikka](https://github.com/iconara/mikka) is for Akka's Java API.

The goal of Heller is to make the Producer and Consumer APIs of Kafka a bit more
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

Check the official [Kafka docs](http://kafka.apache.org/documentation.html#producerconfigs) for possible values for each parameter.

To send messages you creates instances of ```Heller::Message``` and feed them to the
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

## Consumer API

```Heller::Consumer``` wraps ```kafka.javaapi.consumer.SimpleConsumer``` and provides
basically the same methods, but with a bit more convenience (or at least I'd
like to think so).

A ```Consumer``` can be created in the following way:

```ruby
require 'heller'

options = {
  # 'generic' consumer options
  :timeout => 5000,            # socket timeout
  :buffer_size => 128 * 1024,  # socket buffer size
  :client_id => 'my-consumer', # id of consumer
  # fetch request related options
  :max_wait => 4500,           # maximum time (ms) the consumer will wait for response of a request
  :min_bytes => 1024           # minimum amount of bytes the server (broker) should return for a fetch request
}

consumer = Heller::Consumer.new('localhost:9092', options)
```

The options specified in the options hash are also described in the official
[Kafka docs](http://kafka.apache.org/documentation.html#consumerconfigs), albeit they're described in the context of their high-level
consumer.

The consumer API exposes the following methods: ```#fetch```, ```#metadata```,
```#offsets_before```, ```#earliest_offset``` and ```#latest_offset```, and
their usage is described below.

### Fetching messages

```ruby
topic = 'my-topic'
partition = offset = 0

fetch_response = consumer.fetch(Heller::FetchRequest.new(topic, partition, offset))

if fetch_response.error? && (error_code = fetch_response.error(topic, partition)) != 0
  puts "Got error #{Heller::Errors.error_for(error_code)}!"
else
  message_enumerator = fetch_response.messages(topic, partition)
  message_enumerator.each do |offset, message_payload|
    puts "#{offset}: #{message_payload}"
  end
end
```

See ```Heller::FetchResponse``` (and the related specs) for usage of other
methods.

It's also possible to pass an array of ```FetchRequest``` objects to ```#fetch```.

```ruby
requests = [0, 1, 2].map { |i| Heller::FetchRequest.new(topic, i, offset) }
fetch_response = consumer.fetch(requests)
```

## Status

The project is currently under development, and I wouldn't really recommend it
to be used in any form of production environment.
There is still quite some work that needs to be done, especially for the Consumer API.
The Producer API is more or less done, for the moment at least.

It's getting there, though I'm mostly doing this during my spare time, which is
sparse at times.

## Copyright

Copyright 2013 Mathias Söderberg

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License. You may obtain a copy of the
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.