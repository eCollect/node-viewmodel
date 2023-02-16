var util = require('util'),
    Repository = require('../base'),
    ViewModel = Repository.ViewModel,
    ConcurrencyError = require('../concurrencyError'),
    mongo = Repository.use('mongodb'),
    mongoVersion = Repository.use('mongodb/package.json').version,
    ObjectId = mongo.ObjectId,
    _ = require('lodash'),
    collections = [];

const { resolvify, rejectify } = require('../helpers').async();

function Mongo (options) {
  Repository.call(this, options);

  var defaults = {
    host: 'localhost',
    port: 27017,
    dbName: 'context'//,
    // heartbeat: 60 * 1000
  };

  _.defaults(options, defaults);

  var defaultOpt = {
    ssl: false
  };

  options.options = options.options || {};

  defaultOpt.useNewUrlParser = true;
  defaultOpt.useUnifiedTopology = true;
  _.defaults(options.options, defaultOpt);

  this.options = options;
}

util.inherits(Mongo, Repository);

_.extend(Mongo.prototype, {

  _connectAsync: async function () {
    var options = this.options;

    var connectionUrl;

    if (options.url) {
      connectionUrl = options.url;
    } else {
      var members = options.servers
        ? options.servers
        : [{host: options.host, port: options.port}];

      var memberString = _(members).map(function(m) { return m.host + ':' + m.port; });
      var authString = options.username && options.password
        ? options.username + ':' + options.password + '@'
        : '';
      var optionsString = options.authSource
        ? '?authSource=' + options.authSource
        : '';

      connectionUrl = 'mongodb://' + authString + memberString + '/' + options.dbName + optionsString;
    }

    var client = new mongo.MongoClient(connectionUrl, options.options);
    const cl = await client.connect();
    this.db = cl.db(cl.s.options.dbName);

    if (!this.db.close)
      this.db.close = cl.close.bind(cl);

    cl.on('serverClosed', () => {
      this.emit('disconnect');
      this.stopHeartbeat();
    });

    this.isConnected = true;
    this.emit('connect');

    if (this.options.heartbeat)
        this.startHeartbeat();
  },

  connect: function (callback) {
    this._connectAsync().then(() => {
      if (callback) callback(null, this);
    }).catch(rejectify(callback))
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
          console.error((new Error ('Heartbeat timeouted after ' + gracePeriod + 'ms (mongodb)')).stack);
          self.disconnect();
        }
      }, gracePeriod);

      self.db.admin().ping()
        .then(() => {
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

    if (!this.db) {
      if (callback) callback(null);
      return;
    }

    this.db.close()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  getNewId: function(callback) {
    this.checkConnection();
    callback(null, new ObjectId().toHexString());
  },

  _getAsync: async function(id) {
    this.checkConnection();

    const data = await this.collection.findOne({ _id: id })
    if (!data)
        return new ViewModel({ id: id }, this);

    var vm = new ViewModel(data, this);
    vm.actionOnCommit = 'update';
    return vm;
  },

  get: function(id, callback) {
    if(_.isFunction(id)) {
      callback = id;
      id = null;
    }

    if (!id)
      id = new ObjectId().toHexString();

    this._getAsync(id)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _findAsync: async function(query, queryOptions) {
    this.checkConnection();

    let vms = await this.collection.find(query, queryOptions).toArray();
    // Map to view models
    vms = _.map(vms, (data) => {
      var vm = new ViewModel(data, this);
      vm.actionOnCommit = 'update';
      return vm;
    });

    return vms;
  },

  find: function(query, queryOptions, callback) {
    this._findAsync(query, queryOptions)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _findOneAsync: async function(query, queryOptions) {
    this.checkConnection();

    const data = await this.collection.findOne(query, queryOptions);
    if (!data)
        return null;

    var vm = new ViewModel(data, this);
    vm.actionOnCommit = 'update';
    return vm;
  },

  findOne: function(query, queryOptions, callback) {
    this._findOneAsync(query, queryOptions)
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _bulkCommitAsync: async function (bulkOperation) {
    this.checkConnection();
    return this.collection.bulkWrite(bulkOperation, { ordered: false });
  },

  bulkCommit: function (vms, callback) {
    if (!vms) return callback(new Error());
    if (vms.length === 0) return callback(null, vms);

    var bulkOperation = [];

    var deletedCount = 0;
    var insertedCount = 0;
    var modifiedCount = 0;

    _.each(vms, function(vm) {
      if (!vm.actionOnCommit) return;
      switch(vm.actionOnCommit) {
        case 'delete':
          if (!vm.has('_hash')) return;
          bulkOperation.push({ deleteOne: { filter: { _id: vm.id, _hash: vm.get('_hash') } } });
          deletedCount++;
          break;
        case 'create':
          vm.set('_hash', new ObjectId().toHexString());
          // obj = vm.toJSON();
          obj = vm.attributes;
          obj._id = obj.id;
          bulkOperation.push({ insertOne: obj });
          vm.actionOnCommit = 'update';
          insertedCount++;
          break;
        case 'update':
          var currentHash = vm.get('_hash');
          vm.set('_hash', new ObjectId().toHexString());
          // obj = vm.toJSON();
          obj = vm.attributes;
          obj._id = obj.id;
          var query = { _id: obj._id };
          if (currentHash) {
            query._hash = currentHash;
          }
          bulkOperation.push({ updateOne: { filter: query, update: { $set: obj }, upsert: !currentHash } });
          vm.actionOnCommit = 'update';
          modifiedCount++;
          break;
        default:
          return;
      }
    });

    if (bulkOperation.length === 0) return callback(null, vms);

    this._bulkCommitAsync(bulkOperation).then((result) => {
      if (result.deletedCount < deletedCount) return callback(new ConcurrencyError());
      if (result.insertedCount < insertedCount) return callback(new ConcurrencyError());
      if ((result.modifiedCount + result.upsertedCount) < modifiedCount) return callback(new ConcurrencyError());
      return callback(null, vms);
    }).catch((err) => {
      if (err.message && err.message.indexOf('duplicate key') >= 0)
          return callback(new ConcurrencyError());

      return callback(err);
    })
  },

  _commitAsync: async function(vm) {
    if (!vm.actionOnCommit) throw new Error('actionOnCommit is not defined!');

    this.checkConnection();

    var obj;

    switch(vm.actionOnCommit) {
      case 'delete': {
          if (!vm.has('_hash'))
              return;
          const result = await this.collection.deleteOne({ _id: vm.id, _hash: vm.get('_hash') }, { writeConcern: { w: 1 } });
          if (result.deletedCount === 0)
              throw new ConcurrencyError();
          return;
      }
      case 'create':
          vm.set('_hash', new ObjectId().toHexString());
          // obj = vm.toJSON();
          obj = vm.attributes;
          obj._id = obj.id;
          await this.collection.insertOne(obj, { writeConcern: { w: 1 } });
          vm.actionOnCommit = 'update';
          return vm;
      case 'update': {
          var currentHash = vm.get('_hash');
          vm.set('_hash', new ObjectId().toHexString());
          // obj = vm.toJSON();
          obj = vm.attributes;
          obj._id = obj.id;
          var query = { _id: obj._id };
          if (currentHash)
            query._hash = currentHash;
          
          const result = await this.collection.updateOne(query, { $set: obj }, { writeConcern: { w: 1 }, upsert: !currentHash });
          if (result.modifiedCount === 0 && result.upsertedCount === 0)
            throw new ConcurrencyError();
        
          vm.actionOnCommit = 'update';
          return vm;
      }
      default:
        throw new Error();
    }
  },

  commit: function(vm, callback) {
    this._commitAsync(vm)
      .then(resolvify(callback))
      .catch((err) => {
          if (err.message && err.message.indexOf('duplicate key') >= 0)
              return callback(new ConcurrencyError());
          return callback(err);
      });
  },

  _ensureIndexesAsync: async function() {
    var indexes = (this.repositorySettings &&  this.repositorySettings.mongodb && this.repositorySettings.mongodb.indexes) ? this.repositorySettings.mongodb.indexes : this.indexes

    if (!this.isConnected || !this.collectionName || !indexes) return;

    for (const index of indexes) {
      var options = index.options ? index.options : {};
      index = index.index ? index.index : index;

      if (typeof index === 'string') {
        var key = index;
        index = {};
        index[key] = 1;
      }

      await this.db.createIndex(this.collectionName, index, options);
    }
  },

  ensureIndexes: function() {
      this._ensureIndexesAsync();
  },

  checkConnection: function(doNotEnsureIndexes) {
    if (this.collection)
      return;

    if (collections.indexOf(this.collectionName) < 0)
      collections.push(this.collectionName)

    // this.db.createCollection(this.collectionName); // precreate the collection so the first commit is faster

    this.collection = this.db.collection(this.collectionName);

    if (doNotEnsureIndexes) return;

    this.ensureIndexes();
  },

  clear: function (callback) {
    this.checkConnection(true);

    if (!this.collection) {
      if (callback) callback(null);
      return;
    }

    this.collection.deleteMany({}, { writeConcern: { w: 1 } })
      .then(resolvify(callback))
      .catch(rejectify(callback));
  },

  _clearAllAsync: function () {
    const promises = [];
    for (const col of collections)
        promises.push(this.db.collection(col).deleteMany({}, { writeConcern: { w: 1 } }));

    return Promise.all(promises);
  },

  clearAll: function (callback) {
    this._clearAllAsync()
      .then(resolvify(callback))
      .catch(rejectify(callback));
  }

});

module.exports = Mongo;
