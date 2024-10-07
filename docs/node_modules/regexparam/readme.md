# regexparam [![Build Status](https://badgen.now.sh/travis/lukeed/regexparam)](https://travis-ci.org/lukeed/regexparam)

> A tiny (308B) utility that converts route patterns into RegExp. Limited alternative to [`path-to-regexp`](https://github.com/pillarjs/path-to-regexp) 🙇

With `regexparam`, you may turn a pathing string (eg, `/users/:id`) into a regular expression.

An object with shape of `{ keys, pattern }` is returned, where `pattern` is the `RegExp` and `keys` is an array of your parameter name(s) in the order that they appeared.

Unlike [`path-to-regexp`](https://github.com/pillarjs/path-to-regexp), this module does not create a `keys` dictionary, nor mutate an existing variable. Also, this only ships a parser, which only accept strings. Similarly, and most importantly, `regexparam` **only** handles basic pathing operators:

* Static (`/foo`, `/foo/bar`)
* Parameter (`/:title`, `/books/:title`, `/books/:genre/:title`)
* Parameter w/ Suffix (`/movies/:title.mp4`, `/movies/:title.(mp4|mov)`)
* Optional Parameters (`/:title?`, `/books/:title?`, `/books/:genre/:title?`)
* Wildcards (`*`, `/books/*`, `/books/:genre/*`)

This module exposes two module definitions:

* **CommonJS**: `dist/regexparam.js`
* **ESModule**: `dist/regexparam.mjs`

## Install

```
$ npm install --save regexparam
```


## Usage

```js
const regexparam = require('regexparam');

// Example param-assignment
function exec(path, result) {
  let i=0, out={};
  let matches = result.pattern.exec(path);
  while (i < result.keys.length) {
    out[ result.keys[i] ] = matches[++i] || null;
  }
  return out;
}


// Parameter, with Optional Parameter
// ---
let foo = regexparam('/books/:genre/:title?')
// foo.pattern => /^\/books\/([^\/]+?)(?:\/([^\/]+?))?\/?$/i
// foo.keys => ['genre', 'title']

foo.pattern.test('/books/horror'); //=> true
foo.pattern.test('/books/horror/goosebumps'); //=> true

exec('/books/horror', foo);
//=> { genre: 'horror', title: null }

exec('/books/horror/goosebumps', foo);
//=> { genre: 'horror', title: 'goosebumps' }


// Parameter, with suffix
// ---
let bar = regexparam('/movies/:title.(mp4|mov)');
// bar.pattern => /^\/movies\/([^\/]+?)\.(mp4|mov)\/?$/i
// bar.keys => ['title']

bar.pattern.test('/movies/narnia'); //=> false
bar.pattern.test('/movies/narnia.mp3'); //=> false
bar.pattern.test('/movies/narnia.mp4'); //=> true

exec('/movies/narnia.mp4', bar);
//=> { title: 'narnia' }


// Wildcard
// ---
let baz = regexparam('users/*');
// baz.pattern => /^\/users\/(.*)\/?$/i
// baz.keys => ['wild']

baz.pattern.test('/users'); //=> false
baz.pattern.test('/users/lukeed'); //=> true

exec('/users/lukeed/repos/new', baz);
//=> { wild: 'lukeed/repos/new' }
```

> **Important:** When matching/testing against a generated RegExp, your path **must** begin with a leading slash (`"/"`)!

## Regular Expressions

For fine-tuned control, you may pass a `RegExp` value directly to `regexparam` as its only parameter.

In these situations, `regexparam` **does not** parse nor manipulate your pattern in any way! Because of this, `regexparam` has no "insight" on your route, and instead trusts your input fully. In code, this means that the return value's `keys` is always equal to `false` and the `pattern` is identical to your input value.

This also means that you must manage and parse your own `keys`~!<br>
You may use [named capture groups](https://javascript.info/regexp-groups#named-groups) or traverse the matched segments manually the "old-fashioned" way:

> **Important:** Please check your target browsers' and target [Node.js runtimes' support](https://node.green/#ES2018-features--RegExp-named-capture-groups)!

```js
// Named capture group
const named = regexparam(/^\/posts[/](?<year>[0-9]{4})[/](?<month>[0-9]{2})[/](?<title>[^\/]+)/i);
const { groups } = named.pattern.exec('/posts/2019/05/hello-world');
console.log(groups);
//=> { year: '2019', month: '05', title: 'hello-world' }

// Widely supported / "Old-fashioned"
const named = regexparam(/^\/posts[/]([0-9]{4})[/]([0-9]{2})[/]([^\/]+)/i);
const [url, year, month, title] = named.pattern.exec('/posts/2019/05/hello-world');
console.log(year, month, title);
//=> 2019 05 hello-world
```


## API

There are two API variants:

1) When passing a `String` input, the `loose` parameter is able to affect the output. [View API](#regexparamstr-loose)

2) When passing a `RegExp` value, that must be `regexparam`'s _only_ argument.<br>
Your pattern is saved as written, so `loose` is ignored entirely. [View API](#regexparamrgx)

### regexparam(str, loose)
Returns: `Object`

Returns a `{ keys, pattern }` object, where `pattern` is a generated `RegExp` instance and `keys` is a list of extracted parameter names.

#### str
Type: `String`

The route/pathing string to convert.

> **Note:** It does not matter if your `str` begins with a `/` &mdash; it will be added if missing.

#### loose
Type: `Boolean`<br>
Default: `false`

Should the `RegExp` match URLs that are longer than the [`str`](#str) pattern itself?<br>
By default, the generated `RegExp` will test that the URL begins and _ends with_ the pattern.

```js
const rgx = require('regexparam');

rgx('/users').pattern.test('/users/lukeed'); //=> false
rgx('/users', true).pattern.test('/users/lukeed'); //=> true

rgx('/users/:name').pattern.test('/users/lukeed/repos'); //=> false
rgx('/users/:name', true).pattern.test('/users/lukeed/repos'); //=> true
```

### regexparam(rgx)
Returns: `Object`

Returns a `{ keys, pattern }` object, where pattern is _identical_ to your `rgx` and `keys` is `false`, always.

#### rgx
Type: `RegExp`

Your RegExp pattern.

> **Important:** This pattern is used _as is_! No parsing or interpreting is done on your behalf.


## Related

- [trouter](https://github.com/lukeed/trouter) - A server-side HTTP router that extends from this module.
- [matchit](https://github.com/lukeed/matchit) - Similar (650B) library, but relies on String comparison instead of `RegExp`s.


## License

MIT © [Luke Edwards](https://lukeed.com)
