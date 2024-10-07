/*
Copyright (c) 2016, salesforce.com, inc. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

/* eslint-disable camelcase */

const _ = require('lodash')

/*
 * @typedef {object} InputStream~Position
 * @property {number} cursor
 * @property {number} line
 * @property {number} column
 */

/**
 * Yield characters from a string
 *
 * @protected
 * @class
 */
class InputStream {
  /**
   * Create a new InputStream
   *
   * @param {string} input
   */
  constructor (input) {
    this.input = input
    this.cursor = 0
    this.line = 1
    this.column = 0
  }
  /**
   * Return an object that contains the currrent cursor, line, and column
   *
   * @public
   * @returns {InputStream~Position}
   */
  position () {
    return Object.freeze({
      cursor: this.cursor,
      line: this.line,
      column: this.column
    })
  }
  /**
   * Return the current character with an optional offset
   *
   * @public
   * @param {number} offset
   * @returns {string}
   */
  peek (offset) {
    let cursor = _.isInteger(offset)
      ? this.cursor + offset : this.cursor
    return this.input.charAt(cursor)
  }
  /**
   * Return the current character and advance the cursor
   *
   * @public
   * @returns {string}
   */
  next () {
    let c = this.input.charAt(this.cursor++)
    if (c === '\n') {
      this.line++
      this.column = 0
    } else {
      this.column++
    }
    return c
  }
  /**
   * Return true if the stream has reached the end
   *
   * @public
   * @returns {boolean}
   */
  eof () {
    return this.peek() === ''
  }
  /**
   * Throw an error at the current line/column
   *
   * @public
   * @param {string} message
   * @throws Error
   */
  err (msg) {
    throw new Error(`${msg} (${this.line}:${this.column})`)
  }
}

/**
 * @function createInputStreamP
 * @private
 * @param {string} input
 * @returns {InputStreamProxy}
 */
module.exports = (input) => {
  let i = new InputStream(input)
  /**
   * @namespace
   * @borrows InputStream#position as #position
   * @borrows InputStream#peek as #peek
   * @borrows InputStream#next as #next
   * @borrows InputStream#eof as #eof
   * @borrows InputStream#err as #err
   */
  let InputStreamProxy = {
    position () {
      return i.position()
    },
    peek () {
      return i.peek(...arguments)
    },
    next () {
      return i.next()
    },
    eof () {
      return i.eof()
    },
    err () {
      return i.err(...arguments)
    }
  }
  return InputStreamProxy
}
