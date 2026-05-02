const state = {
    sessionId: null,
    workspaceRoot: "",
    currentPath: ".",
    selectedPath: ".",
    selectedType: "dir",
    model: "",
    permissions: {
        read_enabled: true,
        write_enabled: false,
        shell_enabled: false,
    },
    history: [],
    busy: false,
    treeEntries: {},
    expandedPaths: new Set(["."]),
    previewKind: "empty",
    previewPath: "",
    previewOriginalContent: "",
    previewLineCount: 0,
    previewBytes: 0,
    previewDirty: false,
};

const workspaceFormEl = document.getElementById("workspace-form");
const workspaceRootEl = document.getElementById("workspace-root");
const modelSelectEl = document.getElementById("model-select");
const pickWorkspaceBtnEl = document.getElementById("pick-workspace-btn");
const toggleTreeBtnEl = document.getElementById("toggle-tree-btn");
const closeTreeBtnEl = document.getElementById("close-tree-btn");
const sessionMetaEl = document.getElementById("session-meta");
const permissionsMetaEl = document.getElementById("permissions-meta");
const writeEnabledEl = document.getElementById("write-enabled");
const shellEnabledEl = document.getElementById("shell-enabled");
const workspaceTreeEl = document.getElementById("workspace-tree");
const sidebarTreeDrawerEl = document.getElementById("sidebar-tree-drawer");
const pathBadgeEl = document.getElementById("path-badge");
const refreshWorkspaceBtnEl = document.getElementById("refresh-workspace-btn");
const collapseTreeBtnEl = document.getElementById("collapse-tree-btn");
const clearSessionBtnEl = document.getElementById("clear-session-btn");
const clearPreviewBtnEl = document.getElementById("clear-preview-btn");
const reloadPreviewBtnEl = document.getElementById("reload-preview-btn");
const savePreviewBtnEl = document.getElementById("save-preview-btn");
const previewMetaEl = document.getElementById("preview-meta");
const filePreviewEl = document.getElementById("file-preview");
const messagesEl = document.getElementById("messages");
const chatFormEl = document.getElementById("chat-form");
const messageInputEl = document.getElementById("message-input");
const statusTextEl = document.getElementById("status-text");
const sendBtnEl = document.getElementById("send-btn");
const chatSubtitleEl = document.getElementById("chat-subtitle");

function countLines(text) {
    if (!text) return 0;
    return String(text).split("\n").length;
}

function setTreeDrawerOpen(open) {
    sidebarTreeDrawerEl.classList.toggle("hidden", !open);
}

function getParentPath(relativePath) {
    const text = String(relativePath || "").trim();
    if (!text || text === "." || !text.includes("/")) {
        return ".";
    }
    return text.slice(0, text.lastIndexOf("/")) || ".";
}

function escapeHtml(text) {
    return String(text || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}

function formatBytes(size) {
    if (!Number.isFinite(size)) return "";
    if (size < 1024) return `${size} B`;
    if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
    return `${(size / (1024 * 1024)).toFixed(1)} MB`;
}

function buildFilePreviewMeta() {
    const dirtyLabel = state.previewDirty ? " · 未保存" : "";
    const writeLabel = state.permissions.write_enabled ? " · 可编辑" : " · 只读";
    return `${state.previewPath} · ${state.previewLineCount} 行 · ${formatBytes(state.previewBytes)}${writeLabel}${dirtyLabel}`;
}

function updatePreviewControls() {
    const needsSession = !state.sessionId;
    const isFile = state.previewKind === "file" && !!state.previewPath;
    const canEditFile = isFile && !!state.permissions.write_enabled;

    sendBtnEl.disabled = state.busy || needsSession;
    refreshWorkspaceBtnEl.disabled = state.busy || needsSession;
    collapseTreeBtnEl.disabled = state.busy || needsSession;
    clearSessionBtnEl.disabled = state.busy || needsSession;
    writeEnabledEl.disabled = state.busy || needsSession;
    shellEnabledEl.disabled = state.busy || needsSession;
    reloadPreviewBtnEl.disabled = state.busy || needsSession || state.previewKind === "empty";
    savePreviewBtnEl.disabled = state.busy || !canEditFile || !state.previewDirty;

    filePreviewEl.readOnly = !canEditFile;
}

function setBusy(busy, status = "") {
    state.busy = busy;
    updatePreviewControls();
    if (status) {
        statusTextEl.textContent = status;
    }
}

async function fetchJson(url, options = {}) {
    const response = await fetch(url, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
        throw new Error(data.detail || "请求失败");
    }
    return data;
}

function parseSseBlock(block) {
    const lines = String(block || "").split("\n");
    let eventName = "message";
    const dataLines = [];

    for (const line of lines) {
        if (line.startsWith("event:")) {
            eventName = line.slice(6).trim() || "message";
            continue;
        }
        if (line.startsWith("data:")) {
            dataLines.push(line.slice(5).trimStart());
        }
    }

    const rawData = dataLines.join("\n");
    if (!rawData) {
        return { eventName, payload: {} };
    }

    try {
        return { eventName, payload: JSON.parse(rawData) };
    } catch (error) {
        return { eventName, payload: { raw: rawData } };
    }
}

async function streamSse(url, options = {}, onEvent) {
    const response = await fetch(url, options);
    if (!response.ok) {
        const data = await response.json().catch(() => ({}));
        throw new Error(data.detail || "请求失败");
    }
    if (!response.body) {
        throw new Error("褰撳墠鐜涓嶆敮鎸佹祦寮忓搷搴?");
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
        const { value, done } = await reader.read();
        buffer += decoder.decode(value || new Uint8Array(), { stream: !done });
        buffer = buffer.replace(/\r\n/g, "\n");

        let boundary = buffer.indexOf("\n\n");
        while (boundary !== -1) {
            const block = buffer.slice(0, boundary).trim();
            buffer = buffer.slice(boundary + 2);
            if (block) {
                const parsed = parseSseBlock(block);
                await onEvent(parsed.eventName, parsed.payload);
            }
            boundary = buffer.indexOf("\n\n");
        }

        if (done) {
            const tail = buffer.trim();
            if (tail) {
                const parsed = parseSseBlock(tail);
                await onEvent(parsed.eventName, parsed.payload);
            }
            break;
        }
    }
}

function renderSessionMeta() {
    if (!state.sessionId) {
        sessionMetaEl.textContent = "还没有启动 session。";
        chatSubtitleEl.textContent = "先设置工作目录，再让 agent 开始工作。";
        return;
    }
    sessionMetaEl.textContent = `Session ${state.sessionId.slice(0, 8)} · ${state.workspaceRoot}`;
    chatSubtitleEl.textContent = `工作目录：${state.workspaceRoot} · 当前模型：${state.model}`;
}

function renderPermissions() {
    permissionsMetaEl.textContent = [
        `读目录：${state.permissions.read_enabled ? "已授权" : "未授权"}`,
        `写文件：${state.permissions.write_enabled ? "已授权" : "未授权"}`,
        `终端：${state.permissions.shell_enabled ? "已授权" : "未授权"}`,
    ].join(" · ");

    if (state.previewKind === "file") {
        previewMetaEl.textContent = buildFilePreviewMeta();
    }
}

function renderEmptyPreview(message = "暂无目录信息") {
    state.previewKind = "empty";
    state.previewPath = "";
    state.previewOriginalContent = "";
    state.previewLineCount = 0;
    state.previewBytes = 0;
    state.previewDirty = false;
    previewMetaEl.textContent = "默认显示当前目录摘要，点击左侧文件会切到文件内容预览。";
    filePreviewEl.value = message;
    updatePreviewControls();
}

function renderDirectoryPreview(relativePath, entries = []) {
    const dirs = entries.filter((entry) => entry.type === "dir");
    const files = entries.filter((entry) => entry.type === "file");
    const lines = [
        `# ${relativePath || "."}`,
        "",
        ...dirs.slice(0, 60).map((entry) => `[DIR]  ${entry.name}`),
        ...files.slice(0, 120).map((entry) => `[FILE] ${entry.name}${entry.size ? ` (${formatBytes(entry.size)})` : ""}`),
    ];
    const content = lines.join("\n") || "当前目录为空";

    state.previewKind = "dir";
    state.previewPath = relativePath || ".";
    state.previewOriginalContent = content;
    state.previewLineCount = countLines(content);
    state.previewBytes = new Blob([content]).size;
    state.previewDirty = false;

    previewMetaEl.textContent = `当前目录：${relativePath || "."} · 子目录 ${dirs.length} 个 · 文件 ${files.length} 个`;
    filePreviewEl.value = content;
    updatePreviewControls();
}

function renderMessages() {
    if (!state.history.length) {
        messagesEl.innerHTML = `<div class="empty-state">启动 session 后，这里会显示 agent 的回答以及每次工具调用的结果。</div>`;
        return;
    }

    messagesEl.innerHTML = state.history.map((message) => `
        <article class="message ${message.role}">
            <div class="message-role">${message.role === "user" ? "User" : "Agent"}</div>
            <div class="message-content">${escapeHtml(message.content || (message.isStreaming ? "姝ｅ湪澶勭悊..." : ""))}</div>
            ${message.role === "assistant" && Array.isArray(message.tool_steps) && message.tool_steps.length ? `
                <div class="tool-steps">
                    ${message.tool_steps.map((step, index) => `
                        <details class="tool-step" ${index === message.tool_steps.length - 1 ? "open" : ""}>
                            <summary class="tool-step-head">
                                <span>${escapeHtml(step.tool)} · ${escapeHtml(step.reason || "工具调用")}</span>
                                <span class="meta">${escapeHtml(JSON.stringify(step.args || {}))}</span>
                            </summary>
                            <pre>${escapeHtml(JSON.stringify(step.result, null, 2))}</pre>
                        </details>
                    `).join("")}
                </div>
            ` : ""}
        </article>
    `).join("");

    messagesEl.scrollTop = messagesEl.scrollHeight;
}

function appendLocalAssistantMessage(content) {
    state.history = [
        ...state.history,
        { role: "assistant", content, tool_steps: [] },
    ];
    renderMessages();
}

function appendStreamingAssistantMessage() {
    const assistantMessage = {
        role: "assistant",
        content: "",
        tool_steps: [],
        created_at: Date.now() / 1000,
        isStreaming: true,
    };
    state.history = [...state.history, assistantMessage];
    renderMessages();
    return assistantMessage;
}

function applySession(session) {
    state.sessionId = session?.id || null;
    state.workspaceRoot = session?.workspace_root || "";
    state.model = session?.model || modelSelectEl.value;
    state.permissions = session?.permissions || {
        read_enabled: true,
        write_enabled: false,
        shell_enabled: false,
    };
    state.history = session?.history || [];

    workspaceRootEl.value = state.workspaceRoot;
    modelSelectEl.value = state.model || modelSelectEl.value;
    writeEnabledEl.checked = !!state.permissions.write_enabled;
    shellEnabledEl.checked = !!state.permissions.shell_enabled;

    renderSessionMeta();
    renderPermissions();
    renderMessages();
    updatePreviewControls();
}

function getEntries(path) {
    return state.treeEntries[path] || [];
}

function setEntries(path, entries) {
    state.treeEntries[path] = Array.isArray(entries) ? entries : [];
}

function renderTreeNode(path = ".", depth = 0) {
    const entries = getEntries(path);
    if (!entries.length) {
        return "";
    }

    return `
        <div class="tree-group">
            ${entries.map((entry) => {
                const isDir = entry.type === "dir";
                const isExpanded = isDir && state.expandedPaths.has(entry.relative_path);
                const isSelected = state.selectedPath === entry.relative_path;
                return `
                    <div class="tree-node depth-${depth}">
                        <div class="tree-row ${isSelected ? "selected" : ""}" data-path="${escapeHtml(entry.relative_path)}" data-type="${entry.type}">
                            <button class="tree-toggle ${isDir ? "" : "empty"}" type="button" data-toggle-path="${escapeHtml(entry.relative_path)}">
                                ${isDir ? (isExpanded ? "▾" : "▸") : ""}
                            </button>
                            <button class="tree-label" type="button" data-select-path="${escapeHtml(entry.relative_path)}" data-type="${entry.type}">
                                <span class="tree-icon" aria-hidden="true">${isDir ? (isExpanded ? "📂" : "📁") : "📄"}</span>
                                <span class="tree-kind">${isDir ? "DIR" : "FILE"}</span>
                                <span class="tree-name">${escapeHtml(entry.name)}</span>
                            </button>
                        </div>
                        ${isDir && isExpanded ? `<div class="tree-children">${renderTreeNode(entry.relative_path, depth + 1)}</div>` : ""}
                    </div>
                `;
            }).join("")}
        </div>
    `;
}

function renderWorkspaceTree() {
    pathBadgeEl.textContent = state.currentPath || ".";
    const rootEntries = getEntries(".");
    if (!rootEntries.length) {
        workspaceTreeEl.innerHTML = `<div class="empty-state">当前目录没有可显示的内容。</div>`;
        return;
    }

    workspaceTreeEl.innerHTML = `
        <div class="tree-root">
            <div class="tree-root-label">${escapeHtml(state.workspaceRoot || ".")}</div>
            ${renderTreeNode(".", 0)}
        </div>
    `;

    workspaceTreeEl.querySelectorAll("[data-toggle-path]").forEach((button) => {
        button.addEventListener("click", async (event) => {
            event.stopPropagation();
            await toggleDirectory(button.dataset.togglePath);
        });
    });

    workspaceTreeEl.querySelectorAll("[data-select-path]").forEach((button) => {
        button.addEventListener("click", async () => {
            const path = button.dataset.selectPath;
            const type = button.dataset.type;
            state.selectedPath = path;
            state.selectedType = type;

            if (type === "dir") {
                await toggleDirectory(path, { forceOpen: true, preview: true });
                return;
            }

            renderWorkspaceTree();
            await previewFile(path);
        });
    });
}

async function fetchDirectory(path) {
    const data = await fetchJson(`/api/sessions/${state.sessionId}/workspace?relative_path=${encodeURIComponent(path)}`);
    setEntries(data.relative_path || path, data.entries || []);
    return data;
}

async function fetchFileContent(relativePath) {
    return fetchJson(`/api/sessions/${state.sessionId}/file-content?relative_path=${encodeURIComponent(relativePath)}`);
}

async function toggleDirectory(path, { forceOpen = false, preview = false } = {}) {
    const isExpanded = state.expandedPaths.has(path);

    if (isExpanded && !forceOpen) {
        state.expandedPaths.delete(path);
        if (preview) {
            state.currentPath = path;
            state.selectedPath = path;
            state.selectedType = "dir";
            renderDirectoryPreview(path, getEntries(path));
        }
        renderWorkspaceTree();
        return;
    }

    setBusy(true, `正在读取目录 ${path}...`);
    try {
        const data = await fetchDirectory(path);
        state.expandedPaths.add(path);
        state.currentPath = data.relative_path || path;
        state.selectedPath = state.currentPath;
        state.selectedType = "dir";
        renderWorkspaceTree();
        if (preview) {
            renderDirectoryPreview(state.currentPath, data.entries || []);
        }
        statusTextEl.textContent = `已读取目录：${state.currentPath}`;
    } finally {
        setBusy(false);
    }
}

async function pickWorkspace() {
    setBusy(true, "正在打开本地文件夹选择器...");
    try {
        const data = await fetchJson("/api/pick-workspace", { method: "POST" });
        workspaceRootEl.value = data.workspace_root || "";
        statusTextEl.textContent = "已选择本地文件夹";
    } finally {
        setBusy(false);
    }
}

async function createSession(workspaceRoot, model) {
    setBusy(true, "正在创建 agent session...");
    try {
        const data = await fetchJson("/api/sessions", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ workspace_root: workspaceRoot, model }),
        });

        applySession(data.session);
        state.currentPath = data.workspace_preview.relative_path || ".";
        state.selectedPath = state.currentPath;
        state.selectedType = "dir";
        state.treeEntries = {};
        setEntries(state.currentPath, data.workspace_preview.entries || []);
        state.expandedPaths = new Set(["."]);
        renderWorkspaceTree();
        renderDirectoryPreview(state.currentPath, data.workspace_preview.entries || []);
        statusTextEl.textContent = "Session 已创建";
    } finally {
        setBusy(false);
    }
}

async function updatePermissions() {
    if (!state.sessionId) return;

    setBusy(true, "正在更新授权...");
    try {
        const data = await fetchJson(`/api/sessions/${state.sessionId}/permissions`, {
            method: "PATCH",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                write_enabled: writeEnabledEl.checked,
                shell_enabled: shellEnabledEl.checked,
            }),
        });
        applySession(data.session);
        statusTextEl.textContent = "授权已更新";
    } catch (error) {
        writeEnabledEl.checked = !!state.permissions.write_enabled;
        shellEnabledEl.checked = !!state.permissions.shell_enabled;
        statusTextEl.textContent = error.message || "更新授权失败";
    } finally {
        setBusy(false);
    }
}

async function loadWorkspace(relativePath = ".") {
    if (!state.sessionId) return;
    await toggleDirectory(relativePath, { forceOpen: true, preview: true });
}

async function previewFile(relativePath, { silent = false } = {}) {
    if (!state.sessionId) return;

    if (!silent) {
        setBusy(true, `正在预览 ${relativePath}...`);
    }

    try {
        const data = await fetchFileContent(relativePath);
        state.previewKind = "file";
        state.previewPath = data.relative_path;
        state.previewOriginalContent = data.content || "";
        state.previewLineCount = data.line_count || countLines(data.content || "");
        state.previewBytes = Number.isFinite(data.bytes) ? data.bytes : new Blob([data.content || ""]).size;
        state.previewDirty = false;
        state.selectedPath = data.relative_path;
        state.selectedType = "file";
        filePreviewEl.value = state.previewOriginalContent;
        previewMetaEl.textContent = buildFilePreviewMeta();
        renderWorkspaceTree();
        statusTextEl.textContent = `已预览文件：${relativePath}`;
    } finally {
        if (!silent) {
            setBusy(false);
        } else {
            updatePreviewControls();
        }
    }
}

async function refreshExpandedDirectories() {
    const targets = Array.from(new Set([".", ...state.expandedPaths]));
    for (const path of targets) {
        try {
            await fetchDirectory(path);
        } catch (error) {
            if (path === ".") {
                throw error;
            }
            state.expandedPaths.delete(path);
        }
    }
    renderWorkspaceTree();
}

async function refreshCurrentPreview({ preserveDirtyFile = true } = {}) {
    if (state.previewKind === "dir" && state.previewPath) {
        const data = await fetchDirectory(state.previewPath);
        renderDirectoryPreview(data.relative_path || state.previewPath, data.entries || []);
        return;
    }

    if (state.previewKind === "file" && state.previewPath) {
        if (state.previewDirty && preserveDirtyFile) {
            previewMetaEl.textContent = `${buildFilePreviewMeta()} · 已保留未保存编辑`;
            return;
        }
        await previewFile(state.previewPath, { silent: true });
    }
}

async function refreshWorkspaceViews({ preserveDirtyFile = true } = {}) {
    if (!state.sessionId) return;
    await refreshExpandedDirectories();
    await refreshCurrentPreview({ preserveDirtyFile });
}

function updatePreviewDirtyState() {
    if (state.previewKind !== "file") {
        state.previewDirty = false;
        updatePreviewControls();
        return;
    }

    state.previewDirty = filePreviewEl.value !== state.previewOriginalContent;
    state.previewLineCount = countLines(filePreviewEl.value);
    state.previewBytes = new Blob([filePreviewEl.value]).size;
    previewMetaEl.textContent = buildFilePreviewMeta();
    updatePreviewControls();
}

async function savePreviewEdits() {
    if (!state.sessionId || state.previewKind !== "file" || !state.previewPath) return;
    if (!state.permissions.write_enabled) {
        statusTextEl.textContent = "当前没有写文件权限";
        return;
    }

    setBusy(true, `正在保存 ${state.previewPath}...`);
    try {
        await fetchJson(`/api/sessions/${state.sessionId}/write-file`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
                relative_path: state.previewPath,
                content: filePreviewEl.value,
                mode: "overwrite",
            }),
        });

        state.previewOriginalContent = filePreviewEl.value;
        state.previewDirty = false;
        state.previewLineCount = countLines(filePreviewEl.value);
        state.previewBytes = new Blob([filePreviewEl.value]).size;
        await refreshWorkspaceViews({ preserveDirtyFile: false });
        previewMetaEl.textContent = buildFilePreviewMeta();
        statusTextEl.textContent = `已保存文件：${state.previewPath}`;
    } finally {
        setBusy(false);
    }
}

function collectPossibleWorkspaceChanges(toolSteps = []) {
    const changed = new Set();

    for (const step of toolSteps) {
        const resultPath = step?.result?.relative_path;
        const argPath = step?.args?.relative_path;
        const rawPath = typeof resultPath === "string" && resultPath ? resultPath : argPath;
        if (!rawPath || typeof rawPath !== "string") {
            continue;
        }
        changed.add(rawPath);
        changed.add(getParentPath(rawPath));
    }

    return changed;
}

function addChangedPaths(targetSet, toolSteps = []) {
    for (const path of collectPossibleWorkspaceChanges(toolSteps)) {
        targetSet.add(path);
    }
}

async function sendMessage() {
    if (!state.sessionId || state.busy) return;

    const message = messageInputEl.value.trim();
    if (!message) return;

    state.history = [...state.history, { role: "user", content: message, tool_steps: [] }];
    renderMessages();
    const assistantMessage = appendStreamingAssistantMessage();
    messageInputEl.value = "";

    setBusy(true, "Agent 正在分析工作目录...");
    try {
        let completed = false;
        const changedPaths = new Set();

        await streamSse(`/api/sessions/${state.sessionId}/chat/stream`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message, model: modelSelectEl.value }),
        }, async (eventName, payload) => {
            if (eventName === "start") {
                statusTextEl.textContent = `Agent 已开始本轮分析 · 使用模型 ${payload.model || modelSelectEl.value}`;
                return;
            }

            if (eventName === "status") {
                statusTextEl.textContent = payload.text || "Agent 正在处理中...";
                return;
            }

            if (eventName === "tool_call") {
                assistantMessage.tool_steps = [
                    ...assistantMessage.tool_steps,
                    {
                        tool: payload.tool,
                        reason: payload.reason || "",
                        args: payload.args || {},
                        result: { status: "running" },
                    },
                ];
                renderMessages();
                return;
            }

            if (eventName === "tool_result") {
                const step = {
                    tool: payload.tool,
                    reason: payload.reason || "",
                    args: payload.args || {},
                    result: payload.result || {},
                };
                if (Number.isInteger(payload.index) && payload.index >= 0 && payload.index < assistantMessage.tool_steps.length) {
                    assistantMessage.tool_steps[payload.index] = step;
                } else {
                    assistantMessage.tool_steps = [...assistantMessage.tool_steps, step];
                }
                addChangedPaths(changedPaths, [step]);
                renderMessages();
                return;
            }

            if (eventName === "done") {
                completed = true;
                assistantMessage.isStreaming = false;
                applySession(payload.session);
                addChangedPaths(changedPaths, payload.assistant?.tool_steps || []);
                if (changedPaths.size > 0 || state.previewKind !== "empty") {
                    await refreshWorkspaceViews({ preserveDirtyFile: true });
                }
                statusTextEl.textContent = `Agent 已完成本轮分析 · 使用模型 ${payload.session?.model || state.model}`;
                return;
            }

            if (eventName === "error") {
                throw new Error(payload.detail || "发送失败");
            }
        });

        if (!completed) {
            throw new Error("流式响应提前结束");
        }
    } catch (error) {
        assistantMessage.isStreaming = false;
        assistantMessage.content = `本轮请求失败：${error.message || "发送失败"}`;
        renderMessages();
        statusTextEl.textContent = error.message || "发送失败";
    } finally {
        setBusy(false);
        messageInputEl.focus();
    }
}

workspaceFormEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    const workspaceRoot = workspaceRootEl.value.trim();
    const model = modelSelectEl.value;
    if (!workspaceRoot) {
        statusTextEl.textContent = "请先输入工作目录";
        return;
    }
    await createSession(workspaceRoot, model);
});

pickWorkspaceBtnEl.addEventListener("click", async () => {
    await pickWorkspace();
});

refreshWorkspaceBtnEl.addEventListener("click", async () => {
    if (!state.sessionId) return;
    setBusy(true, "正在刷新工作区...");
    try {
        await refreshWorkspaceViews({ preserveDirtyFile: true });
        statusTextEl.textContent = "工作区已刷新";
    } finally {
        setBusy(false);
    }
});

collapseTreeBtnEl.addEventListener("click", () => {
    state.expandedPaths = new Set(["."]);
    renderWorkspaceTree();
    if (getEntries(".")) {
        state.currentPath = ".";
        state.selectedPath = ".";
        state.selectedType = "dir";
        renderDirectoryPreview(".", getEntries("."));
    }
});

toggleTreeBtnEl.addEventListener("click", () => {
    setTreeDrawerOpen(sidebarTreeDrawerEl.classList.contains("hidden"));
});

closeTreeBtnEl.addEventListener("click", () => {
    setTreeDrawerOpen(false);
});

writeEnabledEl.addEventListener("change", async () => {
    await updatePermissions();
});

shellEnabledEl.addEventListener("change", async () => {
    await updatePermissions();
});

clearSessionBtnEl.addEventListener("click", async () => {
    if (!state.sessionId) return;

    setBusy(true, "正在清空 session...");
    try {
        await fetchJson(`/api/sessions/${state.sessionId}`, { method: "DELETE" });
        state.sessionId = null;
        state.workspaceRoot = "";
        state.currentPath = ".";
        state.selectedPath = ".";
        state.selectedType = "dir";
        state.history = [];
        state.permissions = { read_enabled: true, write_enabled: false, shell_enabled: false };
        state.treeEntries = {};
        state.expandedPaths = new Set(["."]);
        workspaceRootEl.value = "";
        renderSessionMeta();
        renderPermissions();
        renderMessages();
        renderWorkspaceTree();
        renderEmptyPreview();
        statusTextEl.textContent = "Session 已清空";
    } finally {
        setBusy(false);
    }
});

clearPreviewBtnEl.addEventListener("click", () => {
    renderEmptyPreview();
});

reloadPreviewBtnEl.addEventListener("click", async () => {
    if (!state.sessionId || state.previewKind === "empty") return;
    if (state.previewKind === "file" && state.previewDirty) {
        const confirmed = window.confirm("重新加载会丢失当前未保存的修改，确认继续吗？");
        if (!confirmed) return;
    }

    setBusy(true, "正在重新加载预览...");
    try {
        await refreshCurrentPreview({ preserveDirtyFile: false });
        statusTextEl.textContent = "预览已重新加载";
    } finally {
        setBusy(false);
    }
});

savePreviewBtnEl.addEventListener("click", async () => {
    await savePreviewEdits();
});

chatFormEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    await sendMessage();
});

messageInputEl.addEventListener("keydown", async (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
        event.preventDefault();
        await sendMessage();
    }
});

filePreviewEl.addEventListener("input", () => {
    if (state.previewKind !== "file") return;
    updatePreviewDirtyState();
});

filePreviewEl.addEventListener("keydown", async (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === "s") {
        event.preventDefault();
        await savePreviewEdits();
    }
});

renderSessionMeta();
renderPermissions();
renderMessages();
renderWorkspaceTree();
renderEmptyPreview();
setTreeDrawerOpen(false);
