'use strict'

const Promise = require('bluebird')
const fs = require('fs-promise')
const path = require('path')

function parseQuote(src, usernames, aliases) {
  const speakers = new Set()
  const mentions = new Set()

  const usernameRx = new RegExp(usernames.join('|'), 'ig')
  for (const line of src.split(/\n/)) {
    const m = /^\[[^\]]+\]\s+([^:]+):([^]*)/i.exec(line)
    if (m) {
      speakers.add(m[1])

      for (const mention of m[2].match(usernameRx) || []) {
        mentions.add(mention)
      }
    }
  }

  return {
    body: src,
    speakers: Array.from(speakers),
    mentions: Array.from(mentions),
    subjects: []
  }
}

function parseLim(src, usernames, aliases) {
  const lines = src.split(/\n/)
  let bodyLines = lines.slice(0, lines.length - 1)
  let finalLine = lines[lines.length - 1]

  let speakers = [];
  if (/^\s*-/.test(finalLine)) {
    const allNames = usernames.slice()
    allNames.push(...aliases.keys())

    const usernameRx = new RegExp(allNames.join('|'), 'ig')
    speakers = (finalLine.match(usernameRx) || [])
      .map(speaker => speaker.toLowerCase())
      .map(speaker => aliases.get(speaker) || speaker)
      .filter(speaker => speaker !== 'pushbot')
  } else {
    bodyLines = lines
    finalLine = '  - _anonymous_'
  }

  const body = bodyLines.map(line => `> ${line}`).join('\n') + '\n\n' + finalLine

  if (speakers.length === 0) {
    console.log(require('util').inspect({src}, { depth: null }));
  }

  return {body, speakers, mentions: [], subjects: []}
}

class FromQuotefile {

  constructor(filePath, separator, parser, usernames) {
    this.filePath = filePath
    this.separator = separator
    this.parser = parser
    this.usernames = usernames
  }

  load() {
    let aliases = new Map()

    return fs.readFile(path.join(__dirname, '..', 'aliases.json'), {encoding: 'utf8'})
    .then(contents => {
      const aliasData = JSON.parse(contents)
      for (const alias in aliasData) {
        aliases.set(alias, aliasData[alias])
      }

      return fs.readFile(this.filePath, {encoding: 'utf8'})
    })
    .then(contents => {
      return contents.split(this.separator)
        .filter(quote => quote.length > 1)
        .map(src => this.parser(src, this.usernames, aliases))
    })
  }

}

class FromMappingFile {

  constructor(filePath) {
    this.filePath = filePath
  }

  load() {
    return fs.readFile(this.filePath, {encoding: 'utf8'})
    .then(contents => {
      try {
        return JSON.parse(contents)
      } catch (e) {
        console.error(e)
        return {}
      }
    })
  }

}

exports.parseQuote = parseQuote
exports.parseLim = parseLim

exports.FromQuotefile = FromQuotefile
exports.FromMappingFile = FromMappingFile
