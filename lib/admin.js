/*
 * node-rdkafka - Node.js wrapper for RdKafka C/C++ library
 *
 * Copyright (c) 2016 Blizzard Entertainment
 *
 * This software may be modified and distributed under the terms
 * of the MIT license.  See the LICENSE.txt file for details.
 */
'use strict';

module.exports = {
  create: createAdminClient,
};

var Client = require('./client');
var util = require('util');
var Kafka = require('../librdkafka');
var LibrdKafkaError = require('./error');
var shallowCopy = require('./util').shallowCopy;

/**
 * Create a new AdminClient for making topics, partitions, and more.
 *
 * This is a factory method because it immediately starts an
 * active handle with the brokers.
 *
 */
function createAdminClient(conf) {
  var client = new AdminClient(conf);

  // Wrap the error so we throw if it failed with some context
  LibrdKafkaError.wrap(client.connect(), true);

  // Return the client if we succeeded
  return client;
}

/**
 * AdminClient class for administering Kafka
 *
 * This client is the way you can interface with the Kafka Admin APIs.
 * This class should not be made using the constructor, but instead
 * should be made using the factory method.
 *
 * <code>
 * var client = AdminClient.create({ ... });
 * </code>
 *
 * Once you instantiate this object, it will have a handle to the kafka broker.
 * Unlike the other node-rdkafka classes, this class does not ensure that
 * it is connected to the upstream broker. Instead, making an action will
 * validate that.
 *
 * @param {object} conf - Key value pairs to configure the admin client
 * topic configuration
 * @constructor
 */
function AdminClient(conf) {
  if (!(this instanceof AdminClient)) {
    return new AdminClient(conf);
  }

  conf = shallowCopy(conf);

  /**
   * NewTopic model.
   *
   * This is the representation of a new message that is requested to be made
   * using the Admin client.
   *
   * @typedef {object} AdminClient~NewTopic
   * @property {string} topic - the topic name to create
   * @property {number} num_partitions - the number of partitions to give the topic
   * @property {number} replication_factor - the replication factor of the topic
   * @property {object} config - a list of key values to be passed as configuration
   * for the topic.
   */

  this._client = new Kafka.AdminClient(conf);
  this._isConnected = false;
  this.globalConfig = conf;
}

/**
 * Get client metadata.
 *
 * Note: using a <code>metadataOptions.topic</code> parameter has a potential side-effect.
 * A Topic object will be created, if it did not exist yet, with default options
 * and it will be cached by librdkafka.
 *
 * A subsequent call to create the topic object with specific options (e.g. <code>acks</code>) will return
 * the previous instance and the specific options will be silently ignored.
 *
 * To avoid this side effect, the topic object can be created with the expected options before requesting metadata,
 * or the metadata request can be performed for all topics (by omitting <code>metadataOptions.topic</code>).
 *
 * @param {object} metadataOptions - Metadata options to pass to the client.
 * @param {string} metadataOptions.topic - Topic string for which to fetch
 * metadata
 * @param {number} metadataOptions.timeout - Max time, in ms, to try to fetch
 * metadata before timing out. Defaults to 3000.
 * @param {Client~metadataCallback} cb - Callback to fire with the metadata.
 */

AdminClient.prototype.getMetadata = function(metadataOptions, cb) {
  if (!this._isConnected) {
    return cb(new Error('Client is disconnected'));
  }

  var self = this;

  this._client.getMetadata(metadataOptions || {}, function(err, metadata) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    // No error otherwise
    self._metadata = metadata;

    if (cb) {
      cb(null, metadata);
    }

  });

};

/**
 * Connect using the admin client.
 *
 * Should be run using the factory method, so should never
 * need to be called outside.
 *
 * Unlike the other connect methods, this one is synchronous.
 */
AdminClient.prototype.connect = function() {
  LibrdKafkaError.wrap(this._client.connect(), true);
  this._isConnected = true;
};

/**
 * Disconnect the admin client.
 *
 * This is a synchronous method, but all it does is clean up
 * some memory and shut some threads down
 */
AdminClient.prototype.disconnect = function() {
  LibrdKafkaError.wrap(this._client.disconnect(), true);
  this._isConnected = false;
};

/**
 * Create a topic with a given config.
 *
 * @param {NewTopic} topic - Topic to create.
 * @param {number} timeout - Number of milliseconds to wait while trying to create the topic.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.createTopic = function(topic, timeout, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 5000;
  }

  if (!timeout) {
    timeout = 5000;
  }

  this._client.createTopic(topic, timeout, function(err) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb();
    }
  });
};

/**
 * Delete a topic.
 *
 * @param {string} topic - The topic to delete, by name.
 * @param {number} timeout - Number of milliseconds to wait while trying to delete the topic.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.deleteTopic = function(topic, timeout, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 5000;
  }

  if (!timeout) {
    timeout = 5000;
  }

  this._client.deleteTopic(topic, timeout, function(err) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb();
    }
  });
};

/**
 * Delete a record.
 *
 * @param {string} topic - The topic to delete, by name.
 * @param {number} timeout - Number of milliseconds to wait while trying to delete the topic.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.deleteRecords = function(topic, partition, amount, timeoutRequest, timeoutPoll, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeoutRequest === 'function') {
    cb = timeoutRequest;
    timeoutRequest = 30000;
    timeoutPoll = 5000;
  }

  if (!timeoutRequest) {
    timeoutRequest = 5000;
  }
  if (!timeoutPoll) {
    timeoutPoll = 5000;
  }

  this._client.deleteRecords(topic, partition, amount, timeoutRequest, timeoutPoll, function(err, offsetsDeleted) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      console.log('calloing cb');
      cb(null, offsetsDeleted);
    }
  });
};

/**
 * Create new partitions for a topic.
 *
 * @param {string} topic - The topic to add partitions to, by name.
 * @param {number} totalPartitions - The total number of partitions the topic should have
 *                                   after the request
 * @param {number} timeout - Number of milliseconds to wait while trying to create the partitions.
 * @param {function} cb - The callback to be executed when finished
 */
AdminClient.prototype.createPartitions = function(topic, totalPartitions, timeout, cb) {
  if (!this._isConnected) {
    throw new Error('Client is disconnected');
  }

  if (typeof timeout === 'function') {
    cb = timeout;
    timeout = 5000;
  }

  if (!timeout) {
    timeout = 5000;
  }

  this._client.createPartitions(topic, totalPartitions, timeout, function(err) {
    if (err) {
      if (cb) {
        cb(LibrdKafkaError.create(err));
      }
      return;
    }

    if (cb) {
      cb();
    }
  });
};
