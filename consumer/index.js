import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';
import fs from 'fs';

var consumer = new Kafka.KafkaConsumer({
  'group.id': 'kafka',
  'metadata.broker.list': 'localhost:9092',
}, {});

consumer.connect();

consumer.on('ready', () => {
  console.log('consumer ready..')
  consumer.subscribe(['test']);
  consumer.consume();
}).on('data', function (data) {
  console.log(`received message: ${eventType.fromBuffer(data.value)}`);
  let val = `received message: ${eventType.fromBuffer(data.value)}`;
  fs.appendFileSync('tmp/output.txt', val + "\r\n", err => {
    if (err) {
      console.error(err)
      return
    }
  })
});
