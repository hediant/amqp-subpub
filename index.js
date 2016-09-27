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
		exchange : "EventStreamExchange"(default),
		maxRetryTimes : -1,
		autoReconnect : true
	}
*/
var SubPub = function (url, options) {
	EventEmitter.call(this);
	var self = this;

	// options
	options = options || {};
	var exchangeName_ = options.exchange || "EventStreamExchange";
	var maxRetryTimes_ = options.maxRetryTimes === undefined ? -1 : options.maxRetryTimes;
	var autoReconnect_ = options.autoReconnect === undefined ? true : options.autoReconnect;

	// exchange	
	var exchangeType_ = 'fanout';
	var exchangeOptions_ = {durable: false};
	// subscribe
	var subId_ = null;

	// connection
	var connection_, sender_, receiver_;
	// status
	var close_ = ready_ = block_ = false;

	// re-try times
	var retry_ = 0;

	// create sender and receiver channel
	var createChannels = function (){
		return co(function *(){
			//
			// sender
			//
			sender_ = { channel:null };
			sender_.channel = yield connection_.createChannel();
			sender_.channel.on('drain', function (){
				self.emit('drain');
			});
			yield sender_.channel.assertExchange(exchangeName_, exchangeType_, exchangeOptions_);

			//
			// receiver
			//
			receiver_ = { channel:null, queue:null };
			receiver_.channel = yield connection_.createChannel();
			// assert exchange & queue
			yield receiver_.channel.assertExchange(exchangeName_, exchangeType_, exchangeOptions_);
			receiver_.queue = yield receiver_.channel.assertQueue("", {exclusive: true});

			return null;
		});
	}

	// reconnect
	var tryConnect = function (){
		return new Promise((resolve, reject) => {
			retry_ = 0;
			var connect = function (){
				amqp.connect(url).then(function (connection){
					resolve(connection);
				}, function (err){
					if (!ready_)
						return reject(err);

					if (maxRetryTimes_ === -1 || retry_++ < maxRetryTimes_){
						// re-connect 5s
						setTimeout(connect, 5000);
					}
					else{
						reject(new Error('ER_MAX_RETRY_CONNECT'));
					}
				})
			}

			connect();
		});
	}

	// initialization
	var init = function (){
		return co(function *(){
			// connection
			connection_ = yield tryConnect();
			// do reconnection
			connection_.on('close', function (err){
				if (!autoReconnect_){
					self.emit('close');
					return;
				}

				//				
				// NOTE:
				// A graceful close may be initiated by an operator (e.g., with an admin tool), 
				// or if the server is shutting down; in this case, no 'error' event will be emitted.
				// But, 'close' will also be emitted, after 'error'	
				//				
				co(function *(){
					// amqplib will close connection itself
					close_ = true;
					yield init();
					
					// re-sub if need
					if (subId_){
						subId_ = null;
						yield self.sub();					
					}

					close_ = false;
					if (block_){
						block_ = false;
						self.emit('drain');
					}
				}).catch(err => {
					self.emit('error', err);
				})
			});

			// Handle errors
			// Emitted if the connection closes for a reason other than #close being called or a graceful server-initiated close;
			// such reasons include:
			// 		a protocol transgression the server detected (likely a bug in this library)
			// 		a server error
			// 		a network error
			// 		the server thinks the client is dead due to a missed heartbeat
			//
			connection_.on('error', function (err){
				self.emit('error', err);
			});

			// channels
			yield createChannels();
			
			return null;
		});
	}

	this.close = function(){
		return co(function *(){
			close_ = true;
			yield self.unsub();

			// close sender
			if (sender_ && sender_.channel){
				yield sender_.channel.close();
				sender_ = null;
			}
			// close 
			if (receiver_ && receiver_.channel){
				yield receiver_.channel.close();
				receiver_ = null;
			}
			// close connection
			if (connection_){
				yield connection_.close();
				connection_.removeAllListeners();
				connection_ = null;
			}

			// send close event
			self.emit('close');
		}).catch (err => {
			self.emit('error', err);
		});
	}

	//
	// it will return false if the channel's write buffer is 'full' or 'disconnect'
	// and true otherwise.
	// If it returns false, it will emit a 'drain' event at some later time.
	//
	this.write = function(topic, evtClass, fields){
		if (!ready_)
			throw new Error('ER_NOT_READY');

		var body = {
			topic : topic,
			class : evtClass,
			fields : fields
		};

		if (close_){
			block_ = true;
			return false;
		}

		// send to exchange
		// route key '' means to all subscribers
		return sender_.channel.publish(exchangeName_, '', new Buffer(JSON.stringify(body)), {
			persistent : false,
			mandatory : true
		});
	}

	// 订阅新的事件通知
	this.sub = function(){
		if (!ready_)
			throw new Error('ER_NOT_READY');

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
		if (!ready_)
			throw new Error('ER_NOT_READY');		
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
	co(function *(){
		yield init();

		// emit ready
		ready_ = true;
		self.emit('ready');
		return null;
	}).catch(err => {
		self.emit('error', err);
	})
}

require('util').inherits(SubPub, EventEmitter);
module.exports = SubPub;
