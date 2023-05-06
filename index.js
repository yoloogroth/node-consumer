const cors = require('cors')
const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['service/my-kafka-service:9092', 'my-kafka-service:9092', 'localhost:9092', 'my-kafka-server:9092'],
}):

const producer = kafka.producer()

const app = express();
app.use(cors());
app.options('*', cors());

const port = 8080;
//producer.connect()

//producer.disconnect()
//
app.get('/', (req, res, next) => {
  res.send('kafka api');
});

const run = async (username) => {

    await producer.connect()
//    await producer.send()
    await producer.send({
      topic: 'demo',
      messages: [ 
	{ 
	  'value': `{"name": "${username}" }` 
  	} 
      ],
    })
   await producer.disconnect()
}

app.get('/like', (req, res, next) => {
  const username = req.query.name;
  res.send('like ...' + username);
  run(username).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

app.listen(port,  () => 
	console.log('listening on port ' + port
));
