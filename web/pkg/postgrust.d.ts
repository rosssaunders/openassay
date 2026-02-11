/* tslint:disable */
/* eslint-disable */

export function exec_sql(sql: string): Promise<string>;

export function execute_sql(sql: string): Promise<string>;

export function execute_sql_http(sql: string): Promise<string>;

export function execute_sql_http_json(sql: string): Promise<string>;

export function execute_sql_json(sql: string): Promise<string>;

export function export_state(): string;

export function export_state_snapshot(): string;

export function import_state(snapshot: string): Promise<string>;

export function import_state_snapshot(snapshot: string): Promise<string>;

export function reset_state(): string;

export function reset_state_snapshot(): string;

export function run_sql(sql: string): Promise<string>;

export function run_sql_http(sql: string): Promise<string>;

export function run_sql_http_json(sql: string): Promise<string>;

export function run_sql_json(sql: string): Promise<string>;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly exec_sql: (a: number, b: number) => any;
    readonly execute_sql: (a: number, b: number) => any;
    readonly execute_sql_http: (a: number, b: number) => any;
    readonly execute_sql_http_json: (a: number, b: number) => any;
    readonly execute_sql_json: (a: number, b: number) => any;
    readonly export_state: () => [number, number];
    readonly import_state: (a: number, b: number) => any;
    readonly import_state_snapshot: (a: number, b: number) => any;
    readonly reset_state: () => [number, number];
    readonly run_sql: (a: number, b: number) => any;
    readonly run_sql_http: (a: number, b: number) => any;
    readonly run_sql_http_json: (a: number, b: number) => any;
    readonly run_sql_json: (a: number, b: number) => any;
    readonly export_state_snapshot: () => [number, number];
    readonly reset_state_snapshot: () => [number, number];
    readonly wasm_bindgen_235b235669c92f2___closure__destroy___dyn_core_aae15cec12117509___ops__function__FnMut__web_sys_ce51838307d5163a___features__gen_CloseEvent__CloseEvent____Output_______: (a: number, b: number) => void;
    readonly wasm_bindgen_235b235669c92f2___closure__destroy___dyn_core_aae15cec12117509___ops__function__FnMut__wasm_bindgen_235b235669c92f2___JsValue____Output_______: (a: number, b: number) => void;
    readonly wasm_bindgen_235b235669c92f2___convert__closures_____invoke___wasm_bindgen_235b235669c92f2___JsValue__wasm_bindgen_235b235669c92f2___JsValue_____: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_235b235669c92f2___convert__closures_____invoke___web_sys_ce51838307d5163a___features__gen_CloseEvent__CloseEvent_____: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen_235b235669c92f2___convert__closures_____invoke___wasm_bindgen_235b235669c92f2___JsValue_____: (a: number, b: number, c: any) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
