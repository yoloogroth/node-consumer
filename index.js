const cors = require('cors')
const express = require('express')
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [
'my-kafka-0.my-kafka-headless.yoloogroth.svc.cluster.local:9092'
	  ]
});

const producer = kafka.producer()

const app = express();
app.use(cors());
app.options('*', cors());

const port = 8080;

app.get('/', (req, res, next) => {
  res.send('kafka api - adsoft');
});

const run = async (username) => {

    await producer.connect()
//    await producer.send()
    await producer.send({
      topic: 'test',
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
  res.send({ 'name' : username } );
  run(username).catch(e => console.error(`[example/producer] ${e.message}`, e))

});

app.listen(port,  () => 
	console.log('listening on port ' + port
));
