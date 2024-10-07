module.exports = function (limit) {
	limit = limit || 1;
	var queue=[], wip=0;

	function toAdd(fn) {
		queue.push(fn) > 1 || run(); // initializes if 1st
	}

	function isDone() {
		wip--; // make room for next
		run();
	}

	function run() {
		if (wip < limit && queue.length > 0) {
			queue.shift()(); wip++; // is now WIP
		}
	}

	return [toAdd, isDone];
}
