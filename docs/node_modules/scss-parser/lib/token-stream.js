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
const invariant = require('invariant')

const HEX_PATTERN = /^#([A-Fa-f0-9]{6}|[A-Fa-f0-9]{3})$/

/**
 * Takes a predicate function and returns its inverse
 *
 * @private
 * @param {function} p
 * @returns {function}
 */
let not = (p) => (c) => !p(c)

/**
 * Return a function that matches the provided character
 *
 * @private
 * @param {function} c
 * @returns {function}
 */
let is_char = (c) => (cc) => c === cc

/**
 * Return true if the character matches whitespace
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_whitespace = (c) => '\t\r\n '.indexOf(c) >= 0

/**
 * Return true if the character matches a newline
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_newline = (c) => c === '\n'

/**
 * Return true if the character matches an operator
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_operator = (c) => '+-*/%=&|!~><^'.indexOf(c) >= 0

/**
 * Return true if the provided operated can be repeated
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_operator_repeatable = (c) => '&|='.indexOf(c) >= 0

/**
 * Return true if the character matches a punctuation
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_punctuation = (c) => ',;(){}[]:#.'.indexOf(c) >= 0

/**
 * Return true if the character matches a digit
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_digit = (c) => /[0-9]/i.test(c)

/**
 * Return true if input matches a comment
 *
 * @private
 * @param {InputStreamProxt} input
 * @returns {boolean}
 */
let is_comment_start = (input) =>
  (input.peek() === '/' && (input.peek(1) === '/' || input.peek(1) === '*'))

/**
 * Return true if the character matches the start of an identifier
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_ident_start = (c) => /[a-z_]/i.test(c)

/**
 * Return true if the character matches an identifier
 *
 * @private
 * @param {string} c
 * @returns {boolean}
 */
let is_ident = (c) => /[a-z0-9_-]/i.test(c)

/**
 * Return true if input matches the start of a number
 *
 * @private
 * @param {InputStreamProxt} input
 * @returns {boolean}
 */
let is_number_start = (input) =>
  is_digit(input.peek()) || (input.peek() === '.' && is_digit(input.peek(1)))

/**
 * Return the length of a possible hex color
 *
 * @private
 * @param {InputStreamProxt} input
 * @returns {number|boolean}
 */
let is_hex = (input) => {
  let hex = input.peek()
  if (hex === '#') {
    let _3 = false
    let _6 = false
    while (hex.length < 7) {
      let c = input.peek(hex.length)
      if (_.isEmpty(c)) break
      hex += c
      if (hex.length === 4) _3 = HEX_PATTERN.test(hex)
      if (hex.length === 7) _6 = HEX_PATTERN.test(hex)
    }
    return _6 ? 6 : _3 ? 3 : false
  }
  return false
}

/*
 * @typedef {object} Token
 * @property {string} type
 * @property {string|array} value
 * @property {InputStream~Position} start
 * @property {InputStream~Position} next
 */

/**
 * Yield tokens from an {@link InputStream}
 *
 * @protected
 * @class
 */
class TokenStream {
  /**
   * Create a new InputStream
   *
   * @param {InputStreamProxy} input
   */
  constructor (input) {
    invariant(
      _.isPlainObject(input) && _.has(input, 'next'),
      'TokenStream requires an InputStream'
    )
    this.input = input
    this.tokens = []
  }
  /**
   * Return a new @{link Token}
   *
   * @private
   * @param {string} type
   * @param {string|array} value
   * @param {InputStream~Position} start
   * @returns {Token}
   */
  createToken (type, value, start) {
    return Object.freeze({
      type,
      value,
      start,
      next: this.input.position()
    })
  }
  /**
   * Return the current token with an optional offset
   *
   * @public
   * @param {number} offset
   * @returns {Token}
   */
  peek (offset) {
    if (!this.tokens.length) {
      let token = this.read_next()
      if (token) this.tokens.push(token)
    }
    if (!offset) return this.tokens[0]
    if (offset < this.tokens.length) return this.tokens[offset]
    while (this.tokens.length <= offset) {
      let token = this.read_next()
      if (token) this.tokens.push(token)
      else break
    }
    return this.tokens[offset]
  }
  /**
   * Return the current token and advance the TokenStream
   *
   * @public
   * @returns {Token}
   */
  next () {
    let token = this.tokens.shift()
    return token || this.read_next()
  }
  /**
   * Return true if the stream has reached the end
   *
   * @public
   * @returns {boolean}
   */
  eof () {
    return typeof this.peek() === 'undefined'
  }
  /**
   * Throw an error at the current line/column
   *
   * @public
   * @param {string} message
   * @throws Error
   */
  err () {
    return this.input.err(...arguments)
  }
  /**
   * Parse the next character(s) as a Token
   *
   * @private
   * @returns {Token}
   */
  read_next () {
    if (this.input.eof()) return null
    let c = this.input.peek()
    // Whitespace
    if (is_whitespace(c)) {
      return this.read_whitespace()
    }
    // Comments
    if (is_comment_start(this.input)) {
      return this.read_comment()
    }
    // Number
    if (is_number_start(this.input)) {
      return this.read_number()
    }
    // Hex
    let hex_length = is_hex(this.input)
    if (hex_length) {
      return this.read_hex(hex_length)
    }
    // Punctutation
    if (is_punctuation(c)) {
      return this.read_punctuation()
    }
    // Identifier
    if (is_ident_start(c)) {
      return this.read_ident()
    }
    // Operator
    if (is_operator(c)) {
      return this.read_operator()
    }
    // String
    if (c === '"' || c === '\'') {
      return this.read_string(c)
    }
    // @ keyword
    if (c === '@') {
      return this.read_atkeyword()
    }
    // Variable
    if (c === '$') {
      return this.read_variable()
    }
    this.err(`Can't handle character: "${c}"`)
  }
  /**
   * Advance the input while the prediciate is true
   *
   * @private
   * @param {function} predicate
   * @returns {string}
   */
  read_while (predicate) {
    let s = ''
    while (!this.input.eof() && predicate(this.input.peek())) {
      s += this.input.next()
    }
    return s
  }
  /**
   * Advance the input (consuming escaped characters) until the end character
   * is reached
   *
   * @private
   * @param {string} end
   * @returns {string}
   */
  read_escaped (end) {
    let escaped = false
    let str = ''
    this.input.next()
    while (!this.input.eof()) {
      let c = this.input.next()
      if (escaped) {
        str += c
        escaped = false
      } else if (c === '\\') {
        str += c
        escaped = true
      } else if (c === end) {
        break
      } else {
        str += c
      }
    }
    return str
  }
  /**
   * Advance the input while whitespace characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_whitespace () {
    let start = this.input.position()
    let value = this.read_while(is_whitespace)
    return this.createToken('space', value, start)
  }
  /**
   * Advance the input while comment characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_comment () {
    let start = this.input.position()
    this.input.next()
    switch (this.input.next()) {
      case '/':
        return this.read_comment_single(start)
      case '*':
        return this.read_comment_multi(start)
    }
  }
  /**
   * Advance the input while singleline comment characters are matched
   *
   * @private
   * @params {InputStream~Position} start
   * @returns {Token}
   */
  read_comment_single (start) {
    let value = this.read_while(not(is_newline))
    return this.createToken('comment_singleline', value, start)
  }
  /**
   * Advance the input while multiline comment characters are matched
   *
   * @private
   * @params {InputStream~Position} start
   * @returns {Token}
   */
  read_comment_multi (start) {
    let prev = ''
    let value = ''
    while (!this.input.eof()) {
      let next = this.input.next()
      if (next === '/' && prev === '*') break
      value += prev
      prev = next
    }
    return this.createToken('comment_multiline', value, start)
  }
  /**
   * Advance the input while punctuation characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_punctuation () {
    let start = this.input.position()
    let value = this.input.next()
    return this.createToken('punctuation', value, start)
  }
  /**
   * Advance the input while operators characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_operator () {
    let start = this.input.position()
    let c = this.input.peek()
    let value = is_operator_repeatable(c)
      ? this.read_while(is_char(c)) : this.input.next()
    return this.createToken('operator', value, start)
  }
  /**
   * Advance the input while identifier characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_ident () {
    let start = this.input.position()
    let value = this.read_while(is_ident)
    return this.createToken('identifier', value, start)
  }
  /**
   * Advance the input while string characters are matched
   *
   * @private
   * @param {string} c - " or '
   * @returns {Token}
   */
  read_string (c) {
    let start = this.input.position()
    let value = this.read_escaped(c)
    let type = 'string'
    if (c === '"') type = 'string_double'
    if (c === '\'') type = 'string_single'
    return this.createToken(type, value, start)
  }
  /**
   * Advance the input while number characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_number () {
    let start = this.input.position()
    let hasPoint = false
    let value = this.read_while((c) => {
      if (c === '.') {
        if (hasPoint) return false
        hasPoint = true
        return true
      }
      return is_digit(c)
    })
    return this.createToken('number', value, start)
  }
  /**
   * Advance the input while hex characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_hex (length) {
    let start = this.input.position()
    this.input.next()
    let value = ''
    for (let i = 0; i < length; i++) {
      value += this.input.next()
    }
    return this.createToken('color_hex', value, start)
  }
  /**
   * Advance the input while atkeyword characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_atkeyword () {
    let start = this.input.position()
    this.input.next()
    let value = this.read_while(is_ident)
    return this.createToken('atkeyword', value, start)
  }
  /**
   * Advance the input while variable characters are matched
   *
   * @private
   * @returns {Token}
   */
  read_variable () {
    let start = this.input.position()
    this.input.next()
    let value = this.read_while(is_ident)
    return this.createToken('variable', value, start)
  }
}

/**
 * @function createTokenStream
 * @private
 * @param {InputStreamProxy} input
 * @returns {TokenStreamProxy}
 */
module.exports = (input) => {
  let t = new TokenStream(input)
  /**
   * @namespace
   * @borrows TokenStream#peek as #peek
   * @borrows TokenStream#next as #next
   * @borrows TokenStream#eof as #eof
   * @borrows TokenStream#err as #err
   */
  let TokenStreamProxy = {
    peek () {
      return t.peek(...arguments)
    },
    next () {
      return t.next()
    },
    eof () {
      return t.eof()
    },
    err () {
      return t.err(...arguments)
    },
    /**
     * Yield all tokens from the stream
     *
     * @instance
     * @returns {Token[]}
     */
    all () {
      let tokens = []
      while (!t.eof()) tokens.push(t.next())
      return tokens
    }
  }
  return TokenStreamProxy
}
