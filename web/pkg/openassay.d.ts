/* tslint:disable */
/* eslint-disable */

export function browse_catalog(path: string): Promise<string>;

export function browse_catalog_json(path: string): Promise<string>;

export function exec_sql(sql: string): Promise<string>;

export function execute_sql(sql: string): Promise<string>;

/**
 * Execute a SQL statement and return the result as Arrow IPC file format bytes.
 *
 * The first statement that produces a result set is converted to an Arrow
 * [`RecordBatch`](arrow::record_batch::RecordBatch) and serialised to the
 * Arrow IPC file format.  Returns an empty `Vec` if no result set was
 * produced or if an error occurred.
 *
 * # JavaScript usage (Apache Arrow JS)
 *
 * ```javascript
 * const bytes = await wasm.execute_sql_arrow("SELECT * FROM data");
 * const table = arrow.tableFromIPC(new Uint8Array(bytes));
 * ```
 */
export function execute_sql_arrow(sql: string): Promise<Uint8Array>;

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
    readonly browse_catalog_json: (a: number, b: number) => any;
    readonly browse_catalog: (a: number, b: number) => any;
    readonly execute_sql: (a: number, b: number) => any;
    readonly execute_sql_json: (a: number, b: number) => any;
    readonly run_sql_json: (a: number, b: number) => any;
    readonly exec_sql: (a: number, b: number) => any;
    readonly run_sql: (a: number, b: number) => any;
    readonly execute_sql_http: (a: number, b: number) => any;
    readonly run_sql_http: (a: number, b: number) => any;
    readonly execute_sql_http_json: (a: number, b: number) => any;
    readonly run_sql_http_json: (a: number, b: number) => any;
    readonly execute_sql_arrow: (a: number, b: number) => any;
    readonly export_state: () => [number, number];
    readonly import_state_snapshot: (a: number, b: number) => any;
    readonly import_state: (a: number, b: number) => any;
    readonly reset_state: () => [number, number];
    readonly export_state_snapshot: () => [number, number];
    readonly reset_state_snapshot: () => [number, number];
    readonly wasm_bindgen__closure__destroy__h0542bf633a0fc94b: (a: number, b: number) => void;
    readonly wasm_bindgen__closure__destroy__h02e0416b893994d2: (a: number, b: number) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h81b57fe47c2b566a: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen__convert__closures_____invoke__h47dc44e8393674ea: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h0f43d66b4126d2b7: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h0f43d66b4126d2b7_1: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h0f43d66b4126d2b7_2: (a: number, b: number, c: any) => void;
    readonly wasm_bindgen__convert__closures_____invoke__h0f43d66b4126d2b7_3: (a: number, b: number, c: any) => void;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
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
