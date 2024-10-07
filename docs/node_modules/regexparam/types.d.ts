declare function regexparam(route: string, loose?: boolean): {
	keys: Array<string>,
	pattern: RegExp
}

declare function regexparam(route: RegExp): {
	keys: false,
	pattern: RegExp
}

export default regexparam;
