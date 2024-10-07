declare module 'throttles' {
	type isDone = () => void;
	type toAdd = (fn: Function) => void;
	function throttles(limit?: number): [toAdd, isDone];
	export = throttles;
}

declare module 'throttles/priority' {
	type isDone = () => void;
	type toAdd = (fn: Function, isHigh?: boolean) => void;
	function throttles(limit?: number): [toAdd, isDone];
	export = throttles;
}
