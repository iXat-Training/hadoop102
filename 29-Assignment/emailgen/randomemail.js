var faker = require('faker');
var kafka = require('kafka-node'),
     Producer = kafka.Producer,
     client = new kafka.Client('localhost:2181'),
     producer = new Producer(client);



 //    producer 'on' ready to send payload to kafka.
 producer.on('ready', function(){
   
    setInterval  (sendRandomEmail,1000,producer);
     
 });

 producer.on('error', function(err){});

function sendRandomEmail(producer){
     var fromemail = faker.name.firstName() + "." + faker.name.lastName() + "@enron.com";
    var toemail = faker.name.firstName() + "." + faker.name.lastName() + "@enron.com";
    var subject = faker.hacker.phrase();
    var data = subject + " :" + faker.hacker.ingverb() + "  " + faker.lorem.paragraph();
    var mid = faker.random.uuid();

    if(faker.random.number()%30==0){
           data += "\n" + "You won " + faker.finance.amount() + " rupees in cash, call us at " + faker.phone.phoneNumber() + " to claim your money";
    }
    var email = {
        "fromemail" : fromemail,
        "toemail" : toemail,
        "subject": subject,
        "body" : data,
        "message_id": mid
    };
     payloads = [
         { topic: 'EMAILIN', "messages": email, partition: faker.random.number()%2 }
     ];
     console.log(payloads);
     producer.send(payloads, function(err, data){
         console.log(data)
     });

}