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

let type = {
  arguments: (n) =>
    '(' + walkValue(n.value) + ')',
  atkeyword: (n) =>
    '@' + n.value,
  attribute: (n) =>
    '[' + walkValue(n.value) + ']',
  block: (n) =>
    '{' + walkValue(n.value) + '}',
  class: (n) =>
    '.' + walkValue(n.value),
  color_hex: (n) =>
    '#' + n.value,
  id: (n) =>
    '#' + walkValue(n.value),
  interpolation: (n) =>
    '#{' + walkValue(n.value) + '}',
  comment_multiline: (n) =>
    '/*' + n.value + '*/',
  comment_singleline: (n) =>
    '//' + n.value,
  parentheses: (n) =>
    '(' + walkValue(n.value) + ')',
  pseudo_class: (n) =>
    ':' + walkValue(n.value),
  psuedo_element: (n) =>
    '::' + walkValue(n.value),
  string_double: (n) =>
   `"${n.value}"`,
  string_single: (n) =>
   `'${n.value}'`,
  variable: (n) =>
    '$' + n.value
}

let walkNode = (node) => {
  if (type[node.type]) return type[node.type](node)
  if (_.isString(node.value)) return node.value
  if (_.isArray(node.value)) return walkValue(node.value)
  return ''
}

let walkValue = (value) => {
  if (!_.isArray(value)) return ''
  return value.reduce((s, node) => {
    return s + walkNode(node)
  }, '')
}

module.exports = (node) => walkNode(node)
