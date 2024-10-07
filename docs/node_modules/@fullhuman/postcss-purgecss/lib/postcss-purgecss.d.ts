/**
 * PostCSS Plugin for PurgeCSS
 *
 * Most bundlers and frameworks to build websites are using PostCSS.
 * The easiest way to configure PurgeCSS is with its PostCSS plugin.
 *
 * @packageDocumentation
 */

import * as postcss from 'postcss';

/**
 * @public
 */
export declare type ComplexSafelist = {
    standard?: StringRegExpArray;
    /**
     * You can safelist selectors and their children based on a regular
     * expression with `safelist.deep`
     *
     * @example
     *
     * ```ts
     * const purgecss = await new PurgeCSS().purge({
     *   content: [],
     *   css: [],
     *   safelist: {
     *     deep: [/red$/]
     *   }
     * })
     * ```
     *
     * In this example, selectors such as `.bg-red .child-of-bg` will be left
     * in the final CSS, even if `child-of-bg` is not found.
     *
     */
    deep?: RegExp[];
    greedy?: RegExp[];
    variables?: StringRegExpArray;
    keyframes?: StringRegExpArray;
};

/**
 * @public
 */
export declare type ExtractorFunction<T = string> = (content: T) => ExtractorResult;

/**
 * @public
 */
export declare type ExtractorResult = ExtractorResultDetailed | string[];

/**
 * @public
 */
export declare interface ExtractorResultDetailed {
    attributes: {
        names: string[];
        values: string[];
    };
    classes: string[];
    ids: string[];
    tags: string[];
    undetermined: string[];
}

/**
 * @public
 */
export declare interface Extractors {
    extensions: string[];
    extractor: ExtractorFunction;
}

/**
 * PostCSS Plugin for PurgeCSS
 *
 * @param opts - PurgeCSS Options
 * @returns the postCSS plugin
 *
 * @public
 */
declare const purgeCSSPlugin: postcss.PluginCreator<UserDefinedOptions>;
export default purgeCSSPlugin;

/**
 * Options used by PurgeCSS to remove unused CSS
 *
 * @public
 */
export declare interface PurgeCSSUserDefinedOptions {
    /** {@inheritDoc purgecss#Options.content} */
    content: Array<string | RawContent>;
    /** {@inheritDoc purgecss#Options.css} */
    css: Array<string | RawCSS>;
    /** {@inheritDoc purgecss#Options.defaultExtractor} */
    defaultExtractor?: ExtractorFunction;
    /** {@inheritDoc purgecss#Options.extractors} */
    extractors?: Array<Extractors>;
    /** {@inheritDoc purgecss#Options.fontFace} */
    fontFace?: boolean;
    /** {@inheritDoc purgecss#Options.keyframes} */
    keyframes?: boolean;
    /** {@inheritDoc purgecss#Options.output} */
    output?: string;
    /** {@inheritDoc purgecss#Options.rejected} */
    rejected?: boolean;
    /** {@inheritDoc purgecss#Options.rejectedCss} */
    rejectedCss?: boolean;
    /** {@inheritDoc purgecss#Options.sourceMap } */
    sourceMap?: boolean | (postcss.SourceMapOptions & { to?: string });
    /** {@inheritDoc purgecss#Options.stdin} */
    stdin?: boolean;
    /** {@inheritDoc purgecss#Options.stdout} */
    stdout?: boolean;
    /** {@inheritDoc purgecss#Options.variables} */
    variables?: boolean;
    /** {@inheritDoc purgecss#Options.safelist} */
    safelist?: UserDefinedSafelist;
    /** {@inheritDoc purgecss#Options.blocklist} */
    blocklist?: StringRegExpArray;
    /** {@inheritDoc purgecss#Options.skippedContentGlobs} */
    skippedContentGlobs?: Array<string>;
    /** {@inheritDoc purgecss#Options.dynamicAttributes} */
    dynamicAttributes?: string[];
}

/**
 * @public
 */
export declare interface RawContent<T = string> {
    extension: string;
    raw: T;
}

/**
 * @public
 */
export declare interface RawCSS {
    raw: string;
    name?: string;
}

/**
 * @public
 */
export declare type StringRegExpArray = Array<RegExp | string>;

/**
 * {@inheritDoc purgecss#UserDefinedOptions}
 *
 * @public
 */
export declare interface UserDefinedOptions extends Omit<PurgeCSSUserDefinedOptions, "content" | "css"> {
    content?: PurgeCSSUserDefinedOptions["content"];
    contentFunction?: (sourceFile: string) => Array<string | RawContent>;
}

/**
 * @public
 */
export declare type UserDefinedSafelist = StringRegExpArray | ComplexSafelist;

export { }
