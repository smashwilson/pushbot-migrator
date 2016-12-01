#!/usr/bin/env node
'use strict'

const Promise = require('bluebird')
const program = require('commander')
const main = require('./index')

program
  .version('1.0.0')
  .option('-r, --redis <url>', 'Redis connection URL [redis://localhost:6379/]', 'redis://localhost:6379/')
  .option('-p, --pg <url>', 'PostgreSQL connection string')
  .option('-t, --transfer', 'Transfer chosen data from Redis to Postgres.')
  .option('-l, --limit <n>', 'Limit chosen operation to first n rows.', parseInt)
  .option('-d, --dump', 'Dump data from Redis to stdout.')
  .parse(process.argv)

main.initialize({ redis: program.redis, pg: program.pg }, program.limit).then((context) => {
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
}).done()
