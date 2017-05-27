'use strict'

const Promise = require('bluebird')
const url = require('url')
const redis = require('redis')
const util = require('util')

Promise.promisifyAll(redis.RedisClient.prototype)

function connect(redisUrl) {
  const info = url.parse(redisUrl, true)
  return redis.createClient(info.port, info.hostname)
}

class FromBrain {

  constructor(redisUrl) {
    this.client = connect(redisUrl)

    this.prefix = 'hubot'
  }

  load() {
    return this.client.getAsync(`${this.prefix}:storage`).then((reply) => JSON.parse(reply))
  }
}

class FromMarkov {

  constructor(redisUrl, prefix) {
    this.client = connect(redisUrl)
    this.prefix = prefix
  }

  withEachTransitionBatch(batchSize, callback) {
    let marker = 0
    let batch = []

    const prefixRx = new RegExp(`^${this.prefix}:`)

    const transitionsFromKey = (key) => {
      const encodedFrom = key.replace(prefixRx, '')
      let i = 1;
      for (; i < encodedFrom.length; i++) {
        let sizePrefix = encodedFrom.slice(0, i)
        if (!/\d+/.test(sizePrefix)) throw new Error(`Invalid prefix: ${sizePrefix}`);
        if (parseInt(sizePrefix) === encodedFrom.length - i) break;
      }

      let from = encodedFrom.slice(i)
      if (from === '') {
        from = ' '
      }

      return this.client.hgetallAsync(key).then(result => {
        const transitions = Object.keys(result).map(to => ({from, to, frequency: result[to]}))
        batch.push(...transitions)

        if (batch.length >= batchSize) {
          const result = batch.slice();
          batch = [];
          callback(result);
        }
      })
    }

    const loop = () => {
      return this.client.scanAsync(marker, 'match', `${this.prefix}:*`).then(([next, batch]) => {
        marker = next
        return Promise.all(batch.map(transitionsFromKey))
      }).then(() => {
        if (marker !== '0') return loop()
      })
    }
    return loop().then(() => batch.length > 0 && callback(batch))
  }

}

exports.FromBrain = FromBrain
exports.FromMarkov = FromMarkov
