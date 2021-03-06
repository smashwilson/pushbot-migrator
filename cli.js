#!/usr/bin/env node
'use strict'

const Promise = require('bluebird')
const program = require('commander')
const main = require('./index')

program
  .version('1.0.0')
  .option('-r, --redis <url>', 'Redis connection URL [redis://localhost:6379/]', 'redis://localhost:6379/')
  .option('-p, --pg <url>', 'PostgreSQL connection string')
  .option('--brain', 'Act on the brain')
  .option('--markov', 'Act on the markov models')
  .option('--quote', 'Act on the quote files')
  .option('--mapping', 'Act on the mapping files')
  .option('-t, --transfer', 'Transfer chosen data from Redis to Postgres.')
  .option('-l, --limit <n>', 'Limit chosen operation to first n rows.', parseInt)
  .option('-d, --dump', 'Dump data from Redis to stdout.')
  .parse(process.argv)

const active = []
if (program.brain) {
  active.push(main.BRAIN)
}
if (program.markov) {
  active.push(main.MARKOV)
}
if (program.quote) {
  active.push(main.QUOTE)
}
if (program.mapping) {
  active.push(main.MAPPING)
}

if (active.length === 0) {
  console.error('At least one of --brain, --markov, or --quote must be specified.')
  program.outputHelp()
  process.exit(1)
}

main.initialize({ redis: program.redis, pg: program.pg }, program.limit, active).then((context) => {
  const tasks = []

  if (program.transfer) {
    tasks.push(context.transfer())
  }
  if (program.dump) {
    tasks.push(context.dump())
  }

  if (tasks.length === 0) {
    console.error('At least one of --transfer or --dump must be specified.')
    program.outputHelp()
    return
  }

  return Promise.all(tasks).finally(() => context.end())
}).then(
  () => {
    console.log('Completed successfully.')
    process.exit(0)
  },
  err => {
    console.error(err)
    process.exit(1)
  }
)
