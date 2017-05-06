'use strict'

const Promise = require('bluebird')
const pg = require('pg-promise')({
  promiseLib: Promise
})
const {FromBrain, FromMarkov} = require('./src/from-redis')
const {FromQuotefile, parseQuote, parseLim} = require('./src/from-quotefile')
const {ToBrain, ToMarkov} = require('./src/to-pg')
const path = require('path')
const util = require('util')

class Context {

  constructor(connections, limit) {
    this.limit = limit

    this.db = pg(connections.pg)
    this.roster = JSON.parse(fs.readFileSync(path.join(__dirname, 'roster.json')))

    this.fromBrain = new FromBrain(connections.redis)
    this.fromForwardMarkov = new FromMarkov(connections.redis, 'markov')
    this.fromReverseMarkov = new FromMarkov(connections.redis, 'remarkov')

    this.fromQuoteFile = new FromQuotefile(path.join(__dirname, 'bundle', 'quotes'), /\n\n/, parseQuote, this.roster)
    this.fromLimFile = new FromQuotefile(path.join(__dirname, 'bundle', 'lim.txt'), /\n---\n/, parseLim, this.roster)

    this.toBrain = new ToBrain(this.db)
    this.toForwardMarkov = new ToMarkov(this.db, 'markov')
    this.toReverseMarkov = new ToMarkov(this.db, 'remarkov')
  }

  prepare() {
    return Promise.all([
      this.toBrain.prepare(),
      this.toForwardMarkov.prepare(),
      this.toReverseMarkov.prepare(),
    ])
  }

  transfer() {
    const transferBrain = () => {
      return this.fromBrain.load()
        .then(storage => this.toBrain.store(storage))
    }

    const transferMarkovModel = (fromModel, toModel) => {
      return fromModel.withEachTransitionBatch(5000, batch => toModel.store(batch))
    }

    const transferForwardModel = transferMarkovModel.bind(this, this.fromForwardMarkov, this.toForwardMarkov)

    const transferReverseModel = transferMarkovModel.bind(this, this.fromReverseMarkov, this.toReverseMarkov)

    return Promise.all([
      transferBrain(),
      transferForwardModel(),
      transferReverseModel(),
    ])
  }

  dump() {
    return this.fromBrain.load().then((storage) => {
      let processed = {}
      if (this.limit !== undefined) {
        let count = 0
        for (let type in storage) {
          processed[type] = {}

          for (let key in storage[type]) {
            const value = storage[type][key]
            processed[type][key] = value

            count++
            if (count >= this.limit) break
          }

          if (count >= this.limit) break
        }
      } else {
        processed = storage
      }

      const output = util.inspect(processed, { maxArrayLength: 5 })
      console.log(`BRAIN:\n${output}`)
    })
  }

  end() {
    pg.end()
  }

}

function initialize(connections, limit) {
  const context = new Context(connections, limit)
  return context.prepare().then(() => context)
}

exports.initialize = initialize
