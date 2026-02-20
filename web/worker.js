// Web Worker: loads WASM SQL engine and handles all SQL execution off the main thread.
// The main thread communicates via postMessage with { type, id, ... } messages.

let mod = null;
let runFunction = null;
let ready = false;
const pending = [];

function resolve(names) {
    for (let i = 0; i < names.length; i++) {
        if (mod && typeof mod[names[i]] === "function") {
            return mod[names[i]];
        }
    }
    return null;
}

async function startup() {
    try {
        mod = await import("./pkg/openassay.js");
        if (typeof mod.default === "function") {
            await mod.default();
        }

        runFunction = resolve([
            "execute_sql_http_json",
            "run_sql_http_json",
            "execute_sql_json",
            "run_sql_json",
            "execute_sql_http",
            "run_sql_http",
            "execute_sql",
            "exec_sql",
            "run_sql",
        ]);

        const httpMode =
            typeof resolve([
                "execute_sql_http_json",
                "run_sql_http_json",
                "execute_sql_http",
                "run_sql_http",
            ]) === "function";

        ready = true;
        self.postMessage({ type: "ready", httpMode: httpMode });

        for (let i = 0; i < pending.length; i++) {
            handleMessage(pending[i]);
        }
        pending.length = 0;
    } catch (err) {
        self.postMessage({
            type: "init_error",
            error: err && err.message ? err.message : String(err),
        });
    }
}

async function handleMessage(msg) {
    const type = msg.type;
    const id = msg.id;

    try {
        let data;
        switch (type) {
            case "exec_sql": {
                if (typeof runFunction !== "function") {
                    self.postMessage({ type: "error", id: id, error: "No execute function available" });
                    return;
                }
                data = await runFunction(msg.sql);
                break;
            }
            case "export_snapshot": {
                const fn = resolve(["export_state_snapshot", "export_state"]);
                if (typeof fn !== "function") {
                    self.postMessage({ type: "error", id: id, error: "Export function not found" });
                    return;
                }
                data = await fn();
                break;
            }
            case "import_snapshot": {
                const fn = resolve(["import_state_snapshot", "import_state"]);
                if (typeof fn !== "function") {
                    self.postMessage({ type: "error", id: id, error: "Import function not found" });
                    return;
                }
                data = await fn(msg.snapshot);
                break;
            }
            case "reset_state": {
                const fn = resolve(["reset_state_snapshot", "reset_state"]);
                if (typeof fn !== "function") {
                    self.postMessage({ type: "error", id: id, error: "Reset function not found" });
                    return;
                }
                data = await fn();
                break;
            }
            default:
                self.postMessage({ type: "error", id: id, error: "Unknown message type: " + type });
                return;
        }
        self.postMessage({ type: "result", id: id, data: data });
    } catch (err) {
        self.postMessage({
            type: "error",
            id: id,
            error: err && err.message ? err.message : String(err),
        });
    }
}

self.onmessage = function (e) {
    if (!ready) {
        pending.push(e.data);
        return;
    }
    handleMessage(e.data);
};

startup();
