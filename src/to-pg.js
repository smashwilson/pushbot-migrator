'use strict'

const Promise = require('bluebird')
const pg = require('pg-promise')({
  promiseLib: Promise
})

const BATCH_SIZE = 5000

class ToBrain {

  constructor(databaseConn) {
    this.db = databaseConn
  }

  log(text) {
    console.log('[BRAIN] ' + text)
  }

  prepare() {
    let create = `
      CREATE TABLE IF NOT EXISTS brain
      (key TEXT, type TEXT, value JSON DEFAULT '{}'::json,
      CONSTRAINT brain_pkey PRIMARY KEY (key, type))
    `

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
}

class ToMarkov {

  constructor(databaseConn, modelName) {
    this.db = databaseConn
    this.modelName = modelName
    this.count = 0
  }

  log(text) {
    console.log(`[MARKOV ${this.modelName}] ${text}`)
  }

  prepare() {
    let sql0 = `
      CREATE TABLE IF NOT EXISTS ${this.modelName}
      ("from" TEXT, "to" TEXT, frequency INTEGER,
      CONSTRAINT ${this.modelName}_pkey PRIMARY KEY ("from", "to"))
    `

    return this.db.none(sql0)
  }

  store(transitions) {
    const cs = new pg.helpers.ColumnSet(
      ['from', 'to', 'frequency'],
      {table: this.modelName}
    )
    const self = this

    return Promise.coroutine(function* () {
      let batch = []

      for (const {from, to, frequency} of transitions) {
        batch.push({from, to, frequency})

        if (batch.length >= BATCH_SIZE) {
          self.count += batch.length
          self.log(`Inserting ${batch.length} rows: ${self.count}`)
          const batchInsert = pg.helpers.insert(batch, cs)
          yield self.db.none(batchInsert)
          batch = []
        }
      }

      // Insert any trailing elements
      if (batch.length > 0) {
        self.count += batch.length
        self.log(`Inserting ${batch.length} rows: ${self.count}`)
        const batchInsert = pg.helpers.insert(batch, cs)
        yield self.db.none(batchInsert)
      }
    })()
  }
}

class ToDocumentSet {

  constructor(databaseConn, setName) {
    this.db = databaseConn
    this.setName = setName;
    this.documentTable = `${setName}_documents`;
    this.attributeTable = `${setName}_attributes`;
  }

  log(text) {
    console.log(`[DOCUMENTSET ${this.setName}] ${text}`);
  }

  prepare() {
    let dtSql = `
      CREATE TABLE IF NOT EXISTS ${this.documentTable}
      (id SERIAL PRIMARY KEY,
      created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      submitter TEXT,
      body TEXT NOT NULL)
    `

    let atSql = `
      CREATE TABLE IF NOT EXISTS ${this.attributeTable}
      (id SERIAL PRIMARY KEY,
      document_id INTEGER REFERENCES ${this.documentTable} ON DELETE CASCADE,
      kind TEXT NOT NULL,
      value TEXT NOT NULL)
    `

    this.dcs = new pg.helpers.ColumnSet(
      ['submitter', 'body'],
      {table: this.documentTable}
    )

    this.acs = new pg.helpers.ColumnSet(
      ['document_id', 'kind', 'value'],
      {table: this.attributeTable}
    )

    return this.db.none(dtSql).then(() => this.db.none(atSql))
  }

  store(documents) {
    this.log(`Inserting ${documents.length} documents.`)
    const bodies = documents.map(document => ({body: document.body, submitter: 'migrated'}))
    const dIns = pg.helpers.insert(bodies, this.dcs) + 'RETURNING id'

    return this.db.many(dIns).then(rows => {
      const attributes = []
      rows.forEach((row, index) => {
        const id = row.id
        const document = documents[index]
        for (const speaker of document.speakers) {
          attributes.push({
            document_id: id,
            kind: 'speaker',
            value: speaker
          })
        }
        for (const mention of document.mentions) {
          attributes.push({
            document_id: id,
            kind: 'mention',
            value: mention
          })
        }
        for (const subject of document.subjects) {
          attributes.push({
            document_id: id,
            kind: 'subject',
            value: subject
          })
        }
      })
      this.log(`Inserting ${attributes.length} attributes.`)

      const aIns = pg.helpers.insert(attributes, this.acs)
      return this.db.none(aIns)
    })
  }
}

exports.ToBrain = ToBrain
exports.ToMarkov = ToMarkov
exports.ToDocumentSet = ToDocumentSet
