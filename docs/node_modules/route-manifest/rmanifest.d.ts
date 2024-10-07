export type Pattern = string;

export interface Asset {
	type: string;
	href: string;
}

export interface Entry<T> {
	files: T[];
	headers: any[];
}

export type FileMap<T = Asset> = Record<Pattern, T[]>;
export type Manifest<T = Asset> = Record<Pattern, Entry<T>>;

declare function rmanifest<T = Asset>(contents: FileMap<T> | Manifest<T>, uri: string, withCommons?: boolean): Entry<T>;

export default rmanifest;
