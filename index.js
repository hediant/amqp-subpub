var amqp = require('amqplib')
		, co = require('co')
		, EventEmitter = require('events').EventEmitter;

/*
evt定义
{
	"topic":"<topic_name>",
	"class":"athena.real",
	"fields":{
		"data":{
			"devid.pointid":"<tag_value>",
			"1.1":13.888,
			... ...
		},
		"recv":"<服务器接收时间，如：1412905743720>",
		"source":"<源时间戳（或者设备时间戳）>",
		"quality":{
			"tag2":"BAD",
			...
		},
		"sender":"<发送者ID>",
		"server":"<服务器ID>"
	}
}
*/

/*
	url - string, default to "amqp://localhost",
	options - {
		exchange : "EventStreamExchange"(default)
	}
*/
var SubPub = function (url, options) {
	EventEmitter.call(this);
	var self = this;

	// exchange
	var exchangeName_ = options.exchange || "EventStreamExchange";
	var exchangeType_ = 'fanout';
	var exchangeOptions_ = {durable: false};
	// subscribe
	var subId_ = null;

	// sender
	var sender_ = { conn:null, channel:null };
	// receiver
	var receiver_ = {conn:null, channel:null, queue:null };

	// initialization
	this.init = function (){
		return co(function *(){
			//
			// sender
			//
			sender_.conn = yield amqp.connect(url);
			sender_.channel = yield sender_.conn.createChannel();
			yield sender_.channel.assertExchange(exchangeName_, exchangeType_, exchangeOptions_);

			//
			// receiver
			//
			receiver_.conn = yield amqp.connect(url);
			receiver_.channel = yield sender_.conn.createChannel();
			// assert exchange & queue
			yield receiver_.channel.assertExchange(exchangeName_, exchangeType_, exchangeOptions_);
			receiver_.queue = yield receiver_.channel.assertQueue("", {exclusive: true});

			// emit ready
			ready_ = true;
			self.emit('ready');
			return null;
		}).catch(err => {
			self.emit('error', err);
		})
	}

	this.close = function(){
		return co(function *(){
			isClose = true;
			yield self.unsub();

			// close sender
			if (sender_.channel)
				yield sender_.channel.close();
			if (sender_.conn)
				yield sender_.conn.close();

			// close 
			if (receiver_.channel)
				yield receiver_.channel.close();
			if (receiver_.conn)
				yield receiver_.conn.close();
		}).catch (err => {
			self.emit('error', err);
		});
	}

	this.write = function(topic, evtClass, fields){
		var body = {
			topic : topic,
			class : evtClass,
			fields : fields
		};

		// send to exchange
		// route key '' means to all subscribers
		return sender_.channel.publish(exchangeName_, '', new Buffer(JSON.stringify(body)), {
			persistent : false,
			mandatory : true
		});
	}

	// 订阅新的事件通知
	this.sub = function(){
		return co(function *(){
			if (subId_)
				return;
			subId_ = Date.now().toString();

			// bind queue to exchange, then we can subscribe messages
			var queueName = receiver_.queue.queue;
			yield receiver_.channel.bindQueue(queueName, exchangeName_, '');

			// consume
			yield receiver_.channel.consume(queueName, function (message){
				try {
					var data = JSON.parse(message.content.toString())
					self.emit('data', data);
				}
				catch(ex){
					console.debug(ex);
				}
			}, {
				consumerTag : subId_,
				noAck : true
			});	
		}).catch(err => {
			self.emit('error', err);
		})

	};

	// 取消订阅新的事件通知
	this.unsub = function() {
		return co(function *(){
			if (subId_){
				var queueName = receiver_.queue.queue;
				yield receiver_.channel.unbindQueue(queueName, exchangeName_, '');
				yield receiver_.channel.cancel(subId_);
				subId_ = null;
			}
		}).catch(err => {
			self.emit('error', err);
		});
	}

	// Do initialization
	this.init();
}

require('util').inherits(SubPub, EventEmitter);
module.exports = SubPub;
