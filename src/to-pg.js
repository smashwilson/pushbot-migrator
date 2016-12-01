'use strict'

const Promise = require('bluebird')
const pg = require('pg-promise')({
  promiseLib: Promise
})

const BATCH_SIZE = 1000

class ToBrain {

  constructor(databaseConn) {
    this.db = pg(databaseConn)
  }

  log(text) {
    console.log('[BRAIN] ' + text)
  }

  prepare() {
    let create = "CREATE TABLE IF NOT EXISTS brain ("
    create += "key TEXT, type TEXT, value JSON DEFAULT '{}'::json, "
    create += "CONSTRAINT brain_pkey PRIMARY KEY (key, type))"

    return this.db.none(create)
      .then(() => this.db.none('TRUNCATE TABLE brain RESTART IDENTITY'))
      .then(() => this.log('Table brain initialized.'))
  }

  store(storage) {
    const cs = new pg.helpers.ColumnSet([
      'type',
      'key',
      { name: 'value', mod: ':json' }
    ], { table: 'brain' })
    const self = this

    return Promise.coroutine(function* () {
      let count = 0
      let batch = []
      for (let type in storage) {
        for (let key in storage[type]) {
          const value = storage[type][key]

          batch.push({type, key, value})
          if (batch.length >= BATCH_SIZE) {
            count += batch.length
            self.log(`Inserting ${batch.length} rows: ${count}`)
            const batchInsert = pg.helpers.insert(batch, cs)
            yield self.db.none(batchInsert)
            batch = []
          }
        }
      }

      // Insert any trailing elements
      if (batch.length > 0) {
        count += batch.length
        self.log(`Final batch of ${batch.length} rows: ${count}`)
        const batchInsert = pg.helpers.insert(batch, cs)
        yield self.db.none(batchInsert)
      }

      self.log('Complete')
    })()
  }

  end() {
    pg.end()
  }

}

class ToMarkov {

  constructor(databaseConn, modelName) {
    this.modelName = modelName
  }

}

exports.ToBrain = ToBrain
exports.ToMarkov = ToMarkov
