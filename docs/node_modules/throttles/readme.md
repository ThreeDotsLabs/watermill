# throttles [![build status](https://badgen.now.sh/github/status/lukeed/throttles)](https://github.com/lukeed/throttles/actions) [![codecov](https://badgen.now.sh/codecov/c/github/lukeed/throttles)](https://codecov.io/gh/lukeed/throttles)

> A tiny (139B to 204B) utility to regulate the execution rate of your functions


## Install

```
$ npm install --save throttles
```


## Modes

There are two "versions" of `throttles`, each of which different purpose:

#### "single"
> **Size (gzip):** 139 bytes<br>
> **Availability:** [UMD](https://unpkg.com/throttles), [CommonJS](https://unpkg.com/throttles/dist/index.js), [ES Module](https://unpkg.com/throttles?module)

This is the primary/default mode, meant for managing single queues.

#### "priority"
> **Size (gzip):** 204 bytes<br>
> **Availability:** [UMD](https://unpkg.com/throttles/priority), [ES Module](https://unpkg.com/throttles/priority/index.mjs)

This is the opt-in mode, meant for managing a low priority _and_ a high priority queue system.<br>
Items within the "high priority" queue are handled before the low/general queue. The `limit` is still enforced.


## Usage

***Selecting a Mode***

```js
// import via npm module
import throttles from 'throttles';
import throttles from 'throttles/priority';

// import via unpkg
import throttles from 'https://unpkg.com/throttles/index.mjs';
import throttles from 'https://unpkg.com/throttles/priority/index.mjs';
```

***Example Usage***

```js
import throttles from 'throttles';

const API = 'https://pokeapi.co/api/v2/pokemon';
const getPokemon = id => fetch(`${API}/${id}`).then(r => r.json());

// Limit concurrency to 3
const [toAdd, isDone] = throttles(3);

// What we'll fetch
const pokemon = ['bulbasaur', 'ivysaur', 'venusaur', 'charmander', 'charmeleon', 'charizard', ...];

// Loop list, enqueuing each Pokemon
// ~> Always keeps 3 requests active at a time
// ~> When complete, marks itself complete via `isDone()`
pokemon.forEach(name => {
	toAdd(() => {
		getPokemon(name).then(isDone);
	});
});

// Or, use `Array.map` to wrap our `getPokemon` function
// ~> This still fetches Pokemon 3 at once
pokemon.map(x => () => getPokemon(x).then(isDone)).forEach(toAdd);
```


## API

### throttles(limit)
Returns: `Array`

Returns a tuple of [[`toAdd`](#toaddfn-ishigh), [`isDone`](#isdone)] actions.

#### limit
Type: `Number`<br>
Default: `1`

The throttle's concurrency limit. By default, runs your functions one at a time.


### toAdd(fn[, isHigh])
Type: `Function`<br>
Returns: `void`

Add a function to the throttle's queue.

> **Important:** In "priority" mode, identical functions are ignored.

#### fn
Type: `Function`<br>
The function to add to the queue.

#### isHigh
Type: `Boolean`<br>
Default: `false`<br>
If the `fn` should be added to the "high priority" queue.

> **Important:** Only available in "priority" mode!


### isDone
Type: `Function`<br>
Returns: `void`

Signifies that a function has been completed.

> **Important:** Failure to call this will prevent `throttles` from continuing to the next item!


## License

MIT © [Luke Edwards](https://lukeed.com)
