builder = require 'redis-builder'

class Queue
  constructor: (@opts) ->
    @redis = builder(@opts.redis)
    @redis_subscriptions = {}
  
  publish: (event, data, callback) ->
    id_key = 'id:' + event
    queue_key = 'q:' + event
    channel_key = 'c:' + event
    
    @redis.incr id_key, (err, id) =>
      return callback(err) if err?

      data.$id = id
      data.$timestamp = new Date().getTime()
      
      data_str = JSON.stringify(data)
      @redis.multi()
        .zadd(queue_key, data.$id, data_str)
        .publish(channel_key, data_str)
        .exec (err) ->
          return callback(err) if err?
          callback(null, data.$id)
  
  read_one_since: (event, id, callback) ->
    @redis.zrangebyscore 'q:' + event, id or '-inf', '+inf', 'limit', 0, 1, (err, items) ->
      return callback(err) if err?
      try
        callback(null, JSON.parse(items[0]))
      catch err
        callback(err)
  
  read_all_since: (event, id, callback) ->
    @redis.zrangebyscore 'q:' + event, (if id? then '(' + id else '-inf'), '+inf', (err, items) ->
      return callback(err) if err?
      try
        callback(null, items.map (i) -> JSON.parse(i))
      catch err
        callback(err)
  
  set_last_seen: (id, callback) ->
    return callback() unless @opts.sow_key?
      @redis.set 'sow:' + @opts.sow_key, id, (err) ->
        callback?(err)
  
  listen: (event, item_callback, connection_callback) ->
    conn = @redis_subscriptions[event]
    return connection_callback?(new Error('You are already listening for ' + event + ' events')) if conn?
    
    queue = []
    send_to_user = (msg) -> queue.push(msg)
    
    sow_key = 'sow:' + @opts.sow_key if @opts.sow_key?
    
    on_message = (channel, msg) =>
      try
        msg = JSON.parse(msg)
        send_to_user(msg)
        # if state of the world is requested, then update the last seen id
        @set_last_seen(msg.$id)
          
      catch err
        err.message = 'Error in parsing messages' + err.message
        console.log(err.stack)
    
    # create connection
    conn = @redis_subscriptions[event] = new builder(@opts.redis)
    # subscribe
    conn.once 'subscribe', =>
      # if state of the world is requested, then lookup the last seen id
      if sow_key?
        @redis.get sow_key, (err, sow_id) =>
          return connection_callback?(err) if err?
          @read_all_since event, sow_id, (err, items) =>
            return connection_callback?(err) if err?
            if items.length > 0
              # send all items first, then send new items to user
              item_callback(i) for i in items
              # update last seen id
              @set_last_seen(items[items.length - 1].$id)
            
            send_to_user = item_callback
      else
        # send messages straight to user
        send_to_user = item_callback
      
      connection_callback?()
    
    conn.on('message', on_message)
    conn.subscribe('c:' + event)

module.exports = Queue
