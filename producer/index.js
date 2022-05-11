import Kafka from 'node-rdkafka';
import eventType from '../eventType.js';

const stream = Kafka.Producer.createWriteStream({
  'metadata.broker.list': 'localhost:9092'
}, {}, {
  topic: 'test'
});

stream.on('error', (err) => {
  console.error('Error in our kafka stream');
  console.error(err);
});

function queueRandomMessage() {
  const category = getRandomUser();
  const action = getRandomAction(category);
  const event = { category, action };
  const success = stream.write(eventType.toBuffer(event));
  if (success) {
    console.log(`message queued (${JSON.stringify(event)})`);
  } else {
    console.log('Too many messages in the queue already..');
  }
}

function getRandomUser() {
  const categories = ['userLogged', 'userLoggedOut'];
  return categories[Math.floor(Math.random() * categories.length)];
}

function getRandomAction(user) {
  if (user === 'userLogged') {
    const actions = ['User Has Logged In', 'A New User Has Logged In'];
    return actions[Math.floor(Math.random() * actions.length)];
  } else if (user === 'userLoggedOut') {
    const actions = ['User Has Logged Out', 'A New User Has Logged Out'];
    return actions[Math.floor(Math.random() * actions.length)];
  } else {
    return 'no actions';
  }
}

setInterval(() => {
  queueRandomMessage();
}, 3000);
