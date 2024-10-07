module.exports = function (limit) {
	limit = limit || 1;
	var qlow=[], idx, qhigh=[], sum=0, wip=0;

	function toAdd(fn, isHigh) {
		if (fn.__t) {
			if (isHigh) {
				idx = qlow.indexOf(fn);
				// must decrement (increments again)
				if (!!~idx) qlow.splice(idx, 1).length && sum--;
			} else return;
		}
		fn.__t = 1;
		(isHigh ? qhigh : qlow).push(fn);
		sum++ || run(); // initializes if 1st
	}

	function isDone() {
		wip--; // make room for next
		run();
	}

	function run() {
		if (wip < limit && sum > 0) {
			(qhigh.shift() || qlow.shift())();
			sum--; wip++; // is now WIP
		}
	}

	return [toAdd, isDone];
}
