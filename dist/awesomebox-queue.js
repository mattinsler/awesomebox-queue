(function() {
  var Queue, builder;

  builder = require('redis-builder');

  Queue = (function() {

    function Queue(opts) {
      this.opts = opts;
      this.redis = builder(this.opts.redis);
      this.redis_subscriptions = {};
    }

    Queue.prototype.publish = function(event, data, callback) {
      var channel_key, id_key, queue_key,
        _this = this;
      id_key = 'id:' + event;
      queue_key = 'q:' + event;
      channel_key = 'c:' + event;
      return this.redis.incr(id_key, function(err, id) {
        var data_str;
        if (err != null) {
          return callback(err);
        }
        data.$id = id;
        data.$timestamp = new Date().getTime();
        data_str = JSON.stringify(data);
        return _this.redis.multi().zadd(queue_key, data.$id, data_str).publish(channel_key, data_str).exec(function(err) {
          if (err != null) {
            return callback(err);
          }
          return callback(null, data.$id);
        });
      });
    };

    Queue.prototype.read_one_since = function(event, id, callback) {
      return this.redis.zrangebyscore('q:' + event, id || '-inf', '+inf', 'limit', 0, 1, function(err, items) {
        if (err != null) {
          return callback(err);
        }
        if (items.length === 0) {
          return callback();
        }
        try {
          return callback(null, JSON.parse(items[0]));
        } catch (err) {
          return callback(err);
        }
      });
    };

    Queue.prototype.read_all_since = function(event, id, callback) {
      return this.redis.zrangebyscore('q:' + event, (id != null ? '(' + id : '-inf'), '+inf', function(err, items) {
        if (err != null) {
          return callback(err);
        }
        try {
          return callback(null, items.map(function(i) {
            return JSON.parse(i);
          }));
        } catch (err) {
          return callback(err);
        }
      });
    };

    Queue.prototype.set_last_seen = function(id, callback) {
      if (this.opts.sow_key == null) {
        return callback();
      }
      return this.redis.set('sow:' + this.opts.sow_key, id, function(err) {
        return typeof callback === "function" ? callback(err) : void 0;
      });
    };

    Queue.prototype.process = function(event, item_callback) {
      var conn, on_message, on_subscribe, process_queue, queue, sow_key, _processing,
        _this = this;
      if (this.opts.sow_key == null) {
        throw new Error('Must provide sow_key to use process');
      }
      queue = [];
      if (this.opts.sow_key != null) {
        sow_key = 'sow:' + this.opts.sow_key;
      }
      _processing = false;
      process_queue = function() {
        var process_item;
        if (_processing === true) {
          return;
        }
        _processing = true;
        process_item = function() {
          var id, item, next;
          item = queue.shift();
          if (item == null) {
            _processing = false;
            return;
          }
          id = item.$id;
          next = function(err) {
            if (err != null) {
              console.log(err.stack);
            }
            return _this.set_last_seen(id, function(err) {
              if (err != null) {
                console.log(err.stack);
              }
              return process.nextTick(process_item);
            });
          };
          return item_callback(item, next);
        };
        return process_item();
      };
      on_subscribe = function() {
        return _this.redis.get(sow_key, function(err, sow_id) {
          if (err != null) {
            throw err;
          }
          return _this.read_all_since(event, sow_id, function(err, items) {
            if (err != null) {
              throw err;
            }
            if (items.length > 0) {
              Array.prototype.unshift.apply(queue, items);
            }
            return process_queue();
          });
        });
      };
      on_message = function(channel, message) {
        var msg;
        try {
          msg = JSON.parse(message);
          queue.push(msg);
          return process_queue();
        } catch (err) {
          err.message = 'Error in parsing messages' + err.message;
          return console.log(err.stack);
        }
      };
      conn = new builder(this.opts.redis);
      conn.once('subscribe', on_subscribe);
      conn.on('message', on_message);
      return conn.subscribe('c:' + event);
    };

    Queue.prototype.listen = function(event, item_callback, connection_callback) {
      var conn, on_message, queue, send_to_user, sow_key,
        _this = this;
      conn = this.redis_subscriptions[event];
      if (conn != null) {
        return typeof connection_callback === "function" ? connection_callback(new Error('You are already listening for ' + event + ' events')) : void 0;
      }
      queue = [];
      send_to_user = function(msg) {
        return queue.push(msg);
      };
      if (this.opts.sow_key != null) {
        sow_key = 'sow:' + this.opts.sow_key;
      }
      on_message = function(channel, msg) {
        try {
          msg = JSON.parse(msg);
          send_to_user(msg);
          return _this.set_last_seen(msg.$id);
        } catch (err) {
          err.message = 'Error in parsing messages' + err.message;
          return console.log(err.stack);
        }
      };
      conn = this.redis_subscriptions[event] = new builder(this.opts.redis);
      conn.once('subscribe', function() {
        if (sow_key != null) {
          _this.redis.get(sow_key, function(err, sow_id) {
            if (err != null) {
              return typeof connection_callback === "function" ? connection_callback(err) : void 0;
            }
            return _this.read_all_since(event, sow_id, function(err, items) {
              var i, _i, _len;
              if (err != null) {
                return typeof connection_callback === "function" ? connection_callback(err) : void 0;
              }
              if (items.length > 0) {
                for (_i = 0, _len = items.length; _i < _len; _i++) {
                  i = items[_i];
                  item_callback(i);
                }
                _this.set_last_seen(items[items.length - 1].$id);
              }
              return send_to_user = item_callback;
            });
          });
        } else {
          send_to_user = item_callback;
        }
        return typeof connection_callback === "function" ? connection_callback() : void 0;
      });
      conn.on('message', on_message);
      return conn.subscribe('c:' + event);
    };

    return Queue;

  })();

  module.exports = Queue;

}).call(this);
