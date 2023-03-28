var util = require('util'),
    Repository = require('../base'),
    ViewModel = Repository.ViewModel,
    ConcurrencyError = require('../concurrencyError'),
    redis = Repository.use('redis'),
    jsondate = require('jsondate'),
    _ = require('lodash'),
    collections = [];

const { v4: uuid } = require('uuid');

    
const { resolvify, rejectify } = require('../helpers').async();

function Redis (options) {
  Repository.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 6379,
    retry_strategy: function (/* retries, cause */) {
      return false;
    }//,
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  if (options.url) {
    var url = require('url').parse(options.url);
    if (url.protocol === 'redis:') {
      if (url.auth) {
        var userparts = url.auth.split(':');
        options.user = userparts[0];
        if (userparts.length === 2) {
          options.password = userparts[1];
        }
      }
      options.host = url.hostname;
      options.port = url.port;
      if (url.pathname) {
        options.db = url.pathname.replace('/', '', 1);
      }
    }
  }

  this.options = options;
}

util.inherits(Redis, Repository);

_.extend(Redis.prototype, {

  connect: function (callback) {
    var self = this;

    var options = this.options;

    this.client = redis.createClient({
			socket: {
				port: options.port || options.socket,
				host: options.host,
        reconnectStrategy: options.retry_strategy,
			},
			database: options.db,
      username: options.username,
      password: options.password,
			// legacyMode: true,
		});

    this.prefix = options.prefix;

    var calledBack = false;

    this.client.on('end', function () {
      self.disconnect();
      self.stopHeartbeat();
    });

    this.client.on('error', function (err) {
      console.log(err);

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    });

    this._connect().then(() => {
      self.emit('connect');

      if (self.options.heartbeat) {
        self.startHeartbeat();
      }

      if (calledBack) return;
      calledBack = true;
      if (callback) callback(null, self);
    }).catch(rejectify(callback));
  },

  _connect: async function() {
		if (!this.client.isOpen)
			await this.client.connect();
  },

  stopHeartbeat: function () {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      delete this.heartbeatInterval;
    }
  },

  startHeartbeat: function () {
    if (this.heartbeatInterval)
      return;

    var self = this;

    var gracePeriod = Math.round(this.options.heartbeat / 2);
    this.heartbeatInterval = setInterval(function () {
      var graceTimer = setTimeout(function () {
        if (self.heartbeatInterval) {
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (redis)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.client.ping().then(() => {
        if (graceTimer) clearTimeout(graceTimer);
      }).catch((err) => {
        if (graceTimer) clearTimeout(graceTimer);
        console.error(err.stack || err);
        self.disconnect();
    });
    }, this.options.heartbeat);
  },

  disconnect: function (callback) {
    this.stopHeartbeat();

    if (this.client && this.client.isOpen)
        this.client.quit();

    this.emit('disconnect');
    if (callback) callback(null, this);
  },

  _getNewIdAsync: async function() {
    await this._connect();
    const id = await this.client.incr('nextItemId:' + this.prefix);
    return id.toString();
  },

  getNewId: function (callback) {
    this.checkConnection();

    this._getNewIdAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _getAsync: async function(id) {
    if (!id)
      id = await this._getNewIdAsync();

    var prefixedId = this.prefix + ':' + id;

    const data = await this.client.get(prefixedId); 
    if (!data)
      return new ViewModel({ id: id }, this);

    const item = jsondate.parse(data.toString());
    var vm = new ViewModel(item, this);
    vm.actionOnCommit = 'update';
    return vm;
  },

  get: function(id, callback) {
    this.checkConnection();

    if(_.isFunction(id)) {
      callback = id;
      id = null;
    }

    this._getAsync(id)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _scan: async function (pattern, handleKeys) {
    await this._connect();

    for await (const key of this.client.scanIterator({
        MATCH: pattern,
    })) {
        await handleKeys(key);
    }
  },

  _findAsync: async function(query, queryOptions) {
    var allKeys = [];

    await this._scan(this.prefix + ':*',
      async function (key) {
        allKeys.push(key);
      });
      
      
    if (queryOptions.skip !== undefined && queryOptions.limit !== undefined) {
      allKeys = allKeys.slice(queryOptions.skip, queryOptions.limit + queryOptions.skip + 1);
    }

    const res = [];
    for (const key of allKeys) {
        const data = await this.client.get(key);
        if (!data)
            continue;
  
        const result = jsondate.parse(data.toString());
        var vm = new ViewModel(result, this);
        vm.actionOnCommit = 'update';
        res.push(vm);
    }

    return res;
  },

  find: function(query, queryOptions, callback) {
    this.checkConnection();

    this._findAsync(query, queryOptions)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _findOneAsync: async function(query, queryOptions) {
    let resultKey;

    await this._scan(this.prefix + ':*',
      async function (key) {
        if (!resultKey)
          resultKey = key;
      });
      
    if (!resultKey)
      return;
      
    const data = await this.client.get(resultKey);
    if (!data)
      return null;
    
    const result = jsondate.parse(data.toString());
    const vm = new ViewModel(result, this);
    vm.actionOnCommit = 'update';

    return vm;
  },

  findOne: function(query, queryOptions, callback) {
    this.checkConnection();

    this._findOneAsync(query, queryOptions)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _commitAsync: async function (vm) {
    if(!vm.actionOnCommit) throw new Error();

    var prefixedId = this.prefix + ':' + vm.id;

    var obj;

    const savedVm = await this._getAsync(vm.id);
    var currentHash = savedVm.get('_hash');
    if (currentHash && vm.has('_hash') && vm.get('_hash') !== currentHash)
        throw new ConcurrencyError();

    await this._connect();
    switch(vm.actionOnCommit) {
      case 'delete': {
        if (!vm.has('_hash'))
            return;

        try {
            await this.client.watch(prefixedId);
            const multi = this.client.multi();
            multi.del(prefixedId);
            await multi.exec();
        } catch (err) {
            throw new ConcurrencyError();
        }
        return;
      }
      case 'create': {
        const data = await this.client.get(prefixedId);
        if (!!data)
            throw new ConcurrencyError();

        vm.set('_hash', uuid().toString());
        obj = JSON.stringify(vm);
        await this.client.set(prefixedId, obj);
        vm.actionOnCommit = 'update';
        return vm;
      }
      case 'update': {
        try {
            await this.client.watch(prefixedId);
            vm.set('_hash', uuid().toString());
            obj = JSON.stringify(vm);
            const multi = this.client.multi();
            multi.set(prefixedId, obj);
            await multi.exec();
        } catch (err) {
          throw new ConcurrencyError();
        }
        vm.actionOnCommit = 'update';
        return vm;
      }
      default:
        throw new Error();
    }

  },

  commit: function (vm, callback) {
    this.checkConnection();
    this._commitAsync(vm)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  checkConnection: function() {
    if (this.collection) {
      return;
    }
    if (collections.indexOf(this.collectionName) < 0) {
      collections.push(this.collectionName);
    }

    this.prefix = this.collectionName;
  },

  _clearAsync: async function() {
    const deletePromises = [];
    await this._connect();
    const keys = await this.client.keys(this.prefix + ':*');    
    for (const key of keys) {
      deletePromises.push(this.client.del(key));
    }

    return Promise.all(deletePromises);
  },

  clear: function (callback) {
    this.checkConnection();

    if (!this.prefix) {
      if (callback) callback(null);
      return;
    }

    this._clearAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _clearAllAsync: async function () {
    await this._connect();
    for (const col of collections) {
        const promises = [];
        promises.push(this.client.del('nextItemId:' + col))
        const keys = await this.client.keys(col + ':*');
        for (const key of keys)
            promises.push(this.client.del(key));
        await Promise.all(promises);
    }
  },

  clearAll: function (callback) {
    this._clearAllAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }
});

module.exports = Redis;
