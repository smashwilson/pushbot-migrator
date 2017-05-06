'use strict'

const Promise = require('bluebird')
const fs = require('fs-promise')

function parseQuote(src, usernames) {
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

function parseLim(src, usernames) {
  const lines = src.split(/\n/)
  const finalLine = lines[lines.length - 1]

  const usernameRx = new RegExp(usernames.join('|'), 'ig')
  return {
    body: src,
    speakers: finalLine.match(usernameRx) || [],
    mentions: [],
    subjects: []
  }
}

class FromQuotefile {

  constructor(filePath, separator, parser, usernames) {
    this.filePath = filePath
    this.separator = separator
    this.parser = parser
    this.usernames = usernames
  }

  load() {
    return fs.readFile(this.filePath, {encoding: 'utf8'})
    .then(contents => {
      return contents.split(this.separator)
        .filter(quote => quote.length > 1)
        .map(src => this.parser(src, this.usernames))
    })
  }

}

exports.parseQuote = parseQuote
exports.parseLim = parseLim

exports.FromQuotefile = FromQuotefile
