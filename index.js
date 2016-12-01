'use strict'

const {FromBrain, FromMarkov} = require('./src/from-redis')
const {ToBrain} = require('./src/to-pg')
const util = require('util')

class Context {

  constructor(connections, limit) {
    this.limit = limit

    this.fromBrain = new FromBrain(connections.redis)
    this.toBrain = new ToBrain(connections.pg)
  }

  prepare() {
    return this.toBrain.prepare()
  }

  transfer() {
    return this.fromBrain.load().then((storage) => this.toBrain.store(storage))
  }

  dump() {
    return this.fromBrain.load().then((storage) => {
      let processed = {}
      if (this.limit === undefined) {
        let count = 0
        for (let type in storage) {
          processed[type] = {}

          for (let key in storage[type]) {
            const value = storage[type][key]
            processed[type][key] = value

            count++
            if (count >= this.limit) break
          }
        }
      } else {
        processed = storage
      }

      const output = util.inspect(processed, { maxArrayLength: 5 })
      console.log(`BRAIN:\n${output}`)
    })
  }

  end() {
    this.toBrain.end()
  }

}

function initialize(connections, limit) {
  const context = new Context(connections, limit)
  return context.prepare().then(() => context)
}

exports.initialize = initialize
