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

/*
 * @typedef {object} Node
 * @property {string} type
 * @property {string|array} value
 * @property {InputStream~Position} start
 * @property {InputStream~Position} next
 */

/**
 * Convert a @{link TokenStreamProxy} to a @{link Node}
 *
 * @protected
 * @class
 */
class Parser {
  /**
   * Create a new InputStream
   *
   * @param {TokenStreamProxy} tokens
   */
  constructor (tokens) {
    this.tokens = tokens
  }
  /**
   * Return a new @{link Node}
   *
   * @private
   * @param {string} type
   * @param {string|array} value
   * @param {InputStream~Position} start
   * @param {InputStream~Position} next
   * @returns {Node}
   */
  createNode (type, value, start, next) {
    return { type, value, start, next }
  }
  /**
   * Return true if the current token(s) are of the provided type
   * and optionally match the specific character(s)
   *
   * @private
   * @param {string} type
   * @param {...string} values
   * @returns {boolean}
   */
  is_type (type, ...values) {
    let t = this.tokens.peek()
    if (!values.length) return t ? type.test(t.type) : false
    return values.reduce((a, c, i) => {
      let t = this.tokens.peek(i)
      return !t ? false : a && type.test(t.type) && t.value === c
    }, true)
  }
  /**
   * Return true if the current token is a space
   *
   * @private
   * @returns {boolean}
   */
  is_space () {
    return this.is_type(/space/)
  }
  /**
   * Return true if the current token is a comment
   *
   * @private
   * @returns {boolean}
   */
  is_comment () {
    return this.is_type(/comment/)
  }
  /**
   * Return true if the current token is a punctuation
   *
   * @private
   * @returns {boolean}
   */
  is_punctuation () {
    return this.is_type(/punctuation/, ...arguments)
  }
  /**
   * Return true if the current token is an operator
   *
   * @private
   * @returns {boolean}
   */
  is_operator () {
    return this.is_type(/operator/, ...arguments)
  }
  /**
   * Return true if the current token is an identifier
   *
   * @private
   * @returns {boolean}
   */
  is_identifier () {
    return this.is_type(/identifier/, ...arguments)
  }
  /**
   * Return true if the current token is an atkeyword
   *
   * @private
   * @returns {boolean}
   */
  is_atkeyword () {
    return this.is_type(/atkeyword/, ...arguments)
  }
  /**
   * Return true if the current tokens are interpolation
   *
   * @private
   * @returns {boolean}
   */
  is_interpolation () {
    return this.is_punctuation('#', '{')
  }
  /**
   * Return the current and next token if the isType predicate succeeds
   *
   * @private
   * @param {string} type
   * @param {function} isType
   * @param {...string} chars
   * @throws Error
   * @returns {boolean}
   */
  skip_type (type, isType, ...chars) {
    if (isType.apply(this, chars)) {
      return { start: this.tokens.peek(), next: this.tokens.next() }
    } else {
      this.tokens.err(`Expecting ${type}: "${chars.join('')}"`)
    }
  }
  /**
   * Expect a punctuation token optionally of the specified type
   *
   * @private
   * @param (...string) chars
   * @throws Error
   * @returns {boolean}
   */
  skip_punctuation () {
    return this.skip_type('punctuation', this.is_punctuation, ...arguments)
  }
  /**
   * Expect an operator token optionally of the specified type
   *
   * @private
   * @param (...string) chars
   * @throws Error
   * @returns {boolean}
   */
  skip_operator () {
    return this.skip_type('operator', this.is_operator, ...arguments)
  }
  /**
   * Expect an atkeyword token
   *
   * @private
   * @throws Error
   * @returns {boolean}
   */
  skip_atkeyword () {
    return this.skip_type('atkeyword', this.is_atkeyword)
  }
  /**
   * Throw an error at the current token
   *
   * @private
   * @throws Error
   */
  unexpected () {
    this.tokens.err(`Unexpected token: "${JSON.stringify(this.input.peek())}"`)
  }
  /**
   * Return a top level stylesheet Node
   *
   * @public
   * @returns {Node}
   */
  parse_stylesheet () {
    let value = []
    while (!this.tokens.eof()) {
      let node = this.parse_node()
      if (_.isArray(node)) {
        value.push(...node)
      } else {
        value.push(node)
      }
    }
    return this.createNode('stylesheet', value)
  }
  /**
   * Parse a top-level Node (atrule,rule,declaration,comment,space)
   *
   * @private
   * @returns {Node|Node[]}
   */
  parse_node () {
    if (
      this.is_space() || this.is_comment()
    ) return this.tokens.next()

    let value = []

    let maybe_declaration = (punctuation) => {
      let expandedPseudo = false
      // If the declaration ends with a ";" expand the first pseudo_class
      // because pseudo_class can't be part of a declaration property
      if (punctuation === ';') {
        let pseudoIndex = _.findIndex(value, {
          type: 'pseudo_class'
        })
        if (pseudoIndex > 0) {
          let a = value[pseudoIndex]
          let b = this.createNode('punctuation', ':', a.start, _.first(a.value).start)
          let nodes = [b].concat(a.value)
          value.splice(pseudoIndex, 1, ...nodes)
          expandedPseudo = true
        }
      }
      // Try to find a ":"
      let puncIndex = _.findIndex(value, {
        type: 'punctuation',
        value: ':'
      })
      // If we found a ":"
      if (puncIndex >= 0) {
        let maybeSpace = value[puncIndex + 1]
        // If we found a space, it wasn't a pseudo class selector,
        // so parse it as a declaration
        // http://www.sassmeister.com/gist/0e60f53033a44b9e5d99362621143059
        if (maybeSpace.type === 'space' || expandedPseudo) {
          let start = _.first(value).start
          let next = _.last(value).next
          let property_ = _.take(value, puncIndex)
          let propertyNode = this.createNode(
            'property', property_, _.first(property_).start, _.last(property_).next)
          let value_ = _.drop(value, puncIndex + 1)
          if (punctuation === '{') {
            let block = this.parse_block()
            value_.push(block)
            next = block.next
          }
          let valueNode = this.createNode(
            'value', value_, _.first(value_).start, _.last(value_).next)
          let declarationValue = [propertyNode, value[puncIndex], valueNode]
          if (punctuation === ';') {
            let { start } = this.skip_punctuation(';')
            declarationValue.push(start)
            next = next.start
          }
          return this.createNode(
            'declaration', declarationValue, start, next)
        }
      }
      return false
    }

    while (!this.tokens.eof()) {
      // AtRule
      if (this.is_atkeyword()) {
        return value.concat(this.parse_at_rule())
      }
      // Atom
      value.push(this.parse_atom())
      // Rule
      if (this.is_punctuation('{')) {
        if (value.length) {
          return maybe_declaration('{') || this.parse_rule(value)
        } else {
          // TODO: throw error?
          return value.concat(this.parse_block())
        }
      }
      // Declaration
      if (this.is_punctuation(';')) {
        return maybe_declaration(';')
      }
    }
    return value
  }
  /**
   * Parse as many atoms as possible while the predicate is true
   *
   * @private
   * @param {function} predicate
   * @returns {Node[]}
   */
  parse_expression (predicate) {
    let value = []
    let declaration = []
    while (true) {
      if (this.tokens.eof() || !predicate()) break
      // Declaration
      if (this.is_punctuation(':') && declaration.length) {
        value.push(this.parse_declaration(declaration))
        // Remove the items that are now a declaration
        value = _.xor(value, declaration)
        declaration = []
      }
      // Atom
      if (this.tokens.eof() || !predicate()) break
      let atom = this.parse_atom()
      value.push(atom)
      // Collect items that might be parsed as a declaration
      // $map: ("red": "blue", "hello": "world");
      switch (atom.type) {
        case 'space':
        case 'punctuation':
          break
        default:
          declaration.push(atom)
      }
    }
    return value
  }
  /**
   * Parse a single atom
   *
   * @private
   * @returns {Node}
   */
  parse_atom () {
    return this.maybe_function(() => {
      // Parens
      if (this.is_punctuation('(')) {
        return this.parse_wrapped('parentheses', '(', ')')
      }
      // Interpolation
      if (this.is_interpolation()) {
        return this.parse_interolation()
      }
      // Attr
      if (this.is_punctuation('[')) {
        return this.parse_wrapped('attribute', '[', ']')
      }
      // Class
      if (this.is_punctuation('.')) {
        return this.parse_selector('class', '.')
      }
      // Id
      if (this.is_punctuation('#')) {
        return this.parse_selector('id', '#')
      }
      // Pseudo Element
      if (this.is_punctuation('::')) {
        return this.parse_selector('pseudo_element', ':')
      }
      // Pseudo Class
      if (this.is_punctuation(':')) {
        let next = this.tokens.peek(1)
        if (
          (next.type === 'identifier') ||
          (next.type === 'punctuation' && next.value === '#')
        ) {
          return this.parse_selector('pseudo_class', ':')
        }
      }
      // Token
      return this.tokens.next()
    })
  }
  /**
   * Parse a declaration
   *
   * @private
   * @param {Node[]} property
   * @returns {Node}
   */
  parse_declaration (property) {
    let { start: firstSeparator } = this.skip_punctuation(':')
    // Expression
    let secondSeparator
    let value = this.parse_expression(() => {
      if (this.is_punctuation(';')) {
        secondSeparator = this.tokens.next()
        return false
      }
      if (this.is_punctuation(',')) {
        secondSeparator = this.tokens.next()
        return false
      }
      if (this.is_punctuation(')')) return false
      return true
    })
    let propertyNode = this.createNode(
      'property', property, _.first(property).start, _.last(property).next)
    let valueNode = this.createNode(
      'value', value, _.first(value).start, _.last(value).next)
    let declarationValue = [propertyNode, firstSeparator, valueNode]
    if (secondSeparator) declarationValue.push(secondSeparator)
    return this.createNode(
      'declaration', declarationValue, _.first(property).start, _.last(value).next)
  }
  /**
   * Parse an expression wrapped in the provided chracters
   *
   * @private
   * @param {string} type
   * @param {string} open
   * @param {string} close
   * @param {InputToken~Position} start
   * @returns {Node}
   */
  parse_wrapped (type, open, close, _start) {
    let { start } = this.skip_punctuation(open)
    let value = this.parse_expression(() =>
      !this.is_punctuation(close)
    )
    let { next } = this.skip_punctuation(close)
    return this.createNode(type, value, (_start || start).start, next.next)
  }
  /**
   * Parse Nodes wrapped in "{}"
   *
   * @private
   * @returns {Node}
   */
  parse_block () {
    let { start } = this.skip_punctuation('{')
    let value = []
    while (
      (!this.tokens.eof()) &&
      (!this.is_punctuation('}'))
    ) {
      let node = this.parse_node()
      if (_.isArray(node)) {
        value.push(...node)
      } else {
        value.push(node)
      }
    }
    let { next } = this.skip_punctuation('}')
    // Sass allows blocks to end with semicolons
    if (this.is_punctuation(';')) {
      this.skip_punctuation(';')
    }
    return this.createNode('block', value, start.start, next.next)
  }
  /**
   * Parse comma separated expressions wrapped in "()"
   *
   * @private
   * @param {string} [type] the type attrribute of the caller
   * @returns {Node}
   */
  parse_arguments (type) {
    let { start } = this.skip_punctuation('(')
    let value = []
    if (type === 'pseudo_class') {
      while (!this.tokens.eof() && !this.is_punctuation(')')) {
        value.push(this.parse_atom())
      }
    } else {
      while (!this.tokens.eof() && !this.is_punctuation(')')) {
        value = value.concat(this.parse_expression(() => {
          if (this.is_punctuation(',')) return false
          if (this.is_punctuation(')')) return false
          return true
        }))
        if (this.is_punctuation(',')) {
          value.push(this.tokens.next())
        }
      }
    }
    let { next } = this.skip_punctuation(')')
    return this.createNode(
      'arguments', value, start.start, next.next)
  }
  /**
   * Optionally wrap a node in a "function"
   *
   * @private
   * @param {function} node - returns a node to optionally be wrapped
   * @returns {Node}
   */
  maybe_function (node) {
    node = node()
    let types = ['identifier', 'function', 'interpolation', 'pseudo_class']
    return this.is_punctuation('(') && _.includes(types, node.type)
      ? this.parse_function(node) : node
  }
  /**
   * Parse a function node
   *
   * @private
   * @params {Node} node - the node to wrap (usually an identifier)
   * @returns {Node}
   */
  parse_function (node) {
    let args = this.parse_arguments(node.type)
    return this.createNode(
      'function', [node, args], node.start, args.next)
  }
  /**
   * Parse interpolation
   *
   * @private
   * @returns {Node}
   */
  parse_interolation () {
    let { start } = this.skip_punctuation('#')
    return this.parse_wrapped('interpolation', '{', '}', start)
  }
  /**
   * Parse an atrule
   *
   * @private
   * @returns {Node}
   */
  parse_at_rule () {
    let { start } = this.skip_atkeyword()
    let value = [start]
    // Space
    if (this.is_space()) value.push(this.tokens.next())
    // Identifier (prevent args being converted to a "function")
    if (this.is_identifier()) value.push(this.tokens.next())
    // Go
    while (!this.tokens.eof()) {
      if (this.is_punctuation('(') && /mixin|include|function/.test(start.value)) {
        value.push(this.parse_arguments())
      }
      if (this.is_punctuation('{')) {
        value.push(this.parse_block())
        break
      }
      if (this.is_punctuation(';')) {
        value.push(this.tokens.next())
        break
      } else {
        value.push(this.parse_atom())
      }
    }
    return this.createNode('atrule', value, start.start, _.last(value).next)
  }
  /**
   * Parse a rule
   *
   * @private
   * @param {Node[]} selectors
   * @returns {Node}
   */
  parse_rule (selectors) {
    let selector = this.createNode(
      'selector', selectors, _.first(selectors).start, _.last(selectors).next)
    let block = this.parse_block()
    return this.createNode(
      'rule', [selector, block], selector.start, block.next)
  }
  /**
   * Parse selector starting with the provided punctuation
   *
   * @private
   * @param {string} type
   * @param {string} punctuation
   * @returns {Node}
   */
  parse_selector (type, punctuation) {
    let { start } = this.skip_punctuation(punctuation)
    // Pseudo Element
    if (this.is_punctuation(':')) {
      this.skip_punctuation(':')
    }
    let value = []
    let next = this.is_interpolation()
      ? this.parse_interolation() : this.tokens.next()
    // Selectors can be a combination of identifiers and interpolation
    while (next.type === 'identifier' || next.type === 'interpolation' || next.type === 'operator') {
      value.push(next)
      next = this.is_interpolation()
        ? this.parse_interolation() : this.tokens.peek()
      if (!next) break
      if (next.type === 'identifier') this.tokens.next()
      // This is usually a dash following interpolation because identifiers
      // can't start with a dash
      if (next.type === 'operator') this.tokens.next()
    }
    if (!value.length) {
      this.tokens.err(`Selector ("${type}") expected "identifier" or "interpolation"`)
    }
    return this.createNode(type, value, start.start, _.last(value).next)
  }
}

/**
 * @function parseTokenStream
 * @private
 * @param {TokenStreamProxt} tokenStream
 * @returns {TokenStreamProxy}
 */
module.exports = (tokenStream) => {
  invariant(
    _.isPlainObject(tokenStream) && _.has(tokenStream, 'next'),
    'Parser requires a TokenStream'
  )
  let parser = new Parser(tokenStream)
  return parser.parse_stylesheet()
}
