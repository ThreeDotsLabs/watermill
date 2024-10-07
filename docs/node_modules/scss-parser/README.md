# SCSS Parser

[![Build Status][travis-image]][travis-url]
[![NPM version][npm-image]][npm-url]

## Getting Started

```javascript
let { parse, stringify } = require('scss-parser')

// Create an AST from a string of SCSS
let ast = parse('.hello { color: $red; }')
// Modify the AST (see below for a better way to do this)
ast.value[0].value[0].value[0].value[0].value = 'world'
// Convert the modified AST back to SCSS
let scss = stringify(ast) // .world { color: $red; }
```

## Traversal

For an easy way to traverse/modify the generated AST, check out [QueryAST](https://github.com/salesforce-ux/query-ast)

```javascript
let { parse, stringify } = require('scss-parser')
let createQueryWrapper = require('query-ast')

// Create an AST
let ast = parse('.hello { color: red; } .world { color: blue; }')
// Create a function to traverse/modify the AST
let $ = createQueryWrapper(ast)
// Make some modifications
$('rule').eq(1).remove()
// Convert the modified AST back to a string
let scss = stringify($().get(0))
```

## Running tests

Clone the repository, then:

```bash
npm install
# requires node >= 5.0.0
npm test
```

## License

Copyright (c) 2016, salesforce.com, inc. All rights reserved.

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

[npm-url]: https://npmjs.org/package/scss-parser
[npm-image]: http://img.shields.io/npm/v/scss-parser.svg

[travis-url]: https://travis-ci.org/salesforce-ux/scss-parser
[travis-image]: https://travis-ci.org/salesforce-ux/scss-parser.svg?branch=master
