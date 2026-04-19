
        const state = {
            auth: null,
            currentView: "chat",
            sessionId: null,
            sessions: [],
            messages: [],
            selectedImages: [],
            selectedFiles: [],
            selectedImage: null,
            sending: false,
            currentAssistantMessageId: null,
            importingKeys: false,
            importingOpus47Keys: false,
            keyCount: 0,
            opus47KeyCount: 0,
            adminTables: [],
            adminDbQuery: "",
            selectedAdminTable: "",
            selectedAdminTableInfo: null,
            selectedAdminRow: null,
            dbModalMode: "view",
            dbModalRow: null,
        };

        const sessionListEl = document.getElementById("session-list");
        const messagesEl = document.getElementById("messages");
        const chatTitleEl = document.getElementById("chat-title");
        const chatSubtitleEl = document.getElementById("chat-subtitle");
        const modelSelectEl = document.getElementById("model-select");
        const modelNoteEl = document.getElementById("model-note");
        const chatFormEl = document.getElementById("chat-form");
        const messageInputEl = document.getElementById("message-input");
        const imageInputEl = document.getElementById("image-input");
        const attachImageBtnEl = document.getElementById("attach-image-btn");
        const removeImageBtnEl = document.getElementById("remove-image-btn");
        const composerImagePreviewEl = document.getElementById("composer-image-preview");
        const composerImageListEl = document.getElementById("composer-image-list");
        const composerImageNameEl = document.getElementById("composer-image-name");
        const imageLightboxEl = document.getElementById("image-lightbox");
        const imageLightboxImgEl = document.getElementById("image-lightbox-img");
        const statusTextEl = document.getElementById("status-text");
        const sendBtnEl = document.getElementById("send-btn");
        const renameSessionBtnEl = document.getElementById("rename-session-btn");
        const deleteSessionBtnEl = document.getElementById("delete-session-btn");
        const importKeysBtnEl = document.getElementById("import-keys-btn");
        const importOpus47KeysBtnEl = document.getElementById("import-opus47-keys-btn");
        const keyMetaEl = document.getElementById("key-meta");
        const keyToolsEl = document.getElementById("key-tools");
        const authOverlayEl = document.getElementById("auth-overlay");
        const adminOverlayEl = document.getElementById("admin-overlay");
        const chatViewEl = document.getElementById("chat-view");
        const dbViewEl = document.getElementById("db-view");
        const chatViewBtnEl = document.getElementById("chat-view-btn");
        const dbViewBtnEl = document.getElementById("db-view-btn");
        const dbRefreshBtnEl = document.getElementById("db-refresh-btn");
        const dbBackChatBtnEl = document.getElementById("db-back-chat-btn");
        const dbSearchInputEl = document.getElementById("db-search-input");
        const dbSearchBtnEl = document.getElementById("db-search-btn");
        const dbClearSearchBtnEl = document.getElementById("db-clear-search-btn");
        const dbModalOverlayEl = document.getElementById("db-modal-overlay");
        const dbModalTitleEl = document.getElementById("db-modal-title");
        const dbModalSubtitleEl = document.getElementById("db-modal-subtitle");
        const dbModalStatusEl = document.getElementById("db-modal-status");
        const dbModalGridEl = document.getElementById("db-modal-grid");
        const closeDbModalBtnEl = document.getElementById("close-db-modal-btn");
        const cancelDbModalBtnEl = document.getElementById("cancel-db-modal-btn");
        const saveDbModalBtnEl = document.getElementById("save-db-modal-btn");
        const authUserEl = document.getElementById("auth-user");
        const authMetaEl = document.getElementById("auth-meta");
        const showAuthBtnEl = document.getElementById("show-auth-btn");
        const logoutBtnEl = document.getElementById("logout-btn");
        const adminBtnEl = document.getElementById("admin-btn");
        const authStatusEl = document.getElementById("auth-status");
        const adminStatusEl = document.getElementById("admin-status");
        const adminStatusOverlayEl = document.getElementById("admin-status-overlay");
        const adminUserListEl = document.getElementById("admin-user-list");
        const adminDbTablesEl = document.getElementById("admin-db-tables");
        const adminDbRowsEl = document.getElementById("admin-db-rows");
        const SUPPORTED_IMAGE_TYPES = new Set(["image/jpeg", "image/png", "image/webp", "image/gif"]);
        const SUPPORTED_DOCUMENT_TYPES = new Set([
            "text/plain",
            "application/pdf",
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ]);
        const DOCUMENT_EXTENSION_TYPES = {
            ".txt": "text/plain",
            ".pdf": "application/pdf",
            ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        };
        const MAX_IMAGE_BYTES = 5 * 1024 * 1024;
        const MAX_DOCUMENT_BYTES = 10 * 1024 * 1024;
        const UPSTREAM_WAITING_TEXT = "正在等待上游响应，通常需要 40-60 秒，请稍候...";
        const UPSTREAM_CONNECTED_TEXT = "已连接上游，正在生成回复...";
        let composerDragDepth = 0;

        function uid() {
            return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
        }

        function setStatus(text) {
            statusTextEl.textContent = text;
        }

        function renderKeyMeta(sourceUrl = null) {
            if (!importKeysBtnEl || !importOpus47KeysBtnEl) {
                return;
            }
            if (keyMetaEl) {
                const sourceText = sourceUrl || "GitHub";
                keyMetaEl.textContent = `Key ${state.keyCount} · ${sourceText}`;
            }
            importKeysBtnEl.disabled = !state.auth?.is_superadmin || state.sending || state.importingKeys;
            importKeysBtnEl.textContent = state.importingKeys ? "Importing Key..." : `导入普通 Key (${state.keyCount})`;
            importOpus47KeysBtnEl.disabled = !state.auth?.is_superadmin || state.sending || state.importingOpus47Keys;
            importOpus47KeysBtnEl.textContent = state.importingOpus47Keys
                ? "Importing Opus4.7 Key..."
                : `导入 Opus4.7 Key (${state.opus47KeyCount})`;
            importKeysBtnEl.textContent = state.importingKeys ? "Importing Key..." : `导入普通 Key (${state.keyCount})`;
        }

        function setAuthStatus(text, isError = false) {
            authStatusEl.textContent = text || "";
            authStatusEl.style.color = isError ? "#dc2626" : "#667085";
        }

        function renderModelNote() {
            if (!modelNoteEl) {
                return;
            }
            const suffix = state.auth?.authenticated
                ? (state.auth?.can_use_opus47 ? "当前账号可直接使用。" : "当前账号选择后会在后端做权限校验。")
                : "登录后会按账号权限校验。";
            modelNoteEl.textContent = `Opus 4.7：适合更复杂的推理和更高质量生成，仅 SuperAdmin 和付费用户可用。${suffix}`;
        }

        function setAdminStatus(text, isError = false) {
            adminStatusEl.textContent = text || "";
            adminStatusEl.style.color = isError ? "#dc2626" : "#667085";
            if (adminStatusOverlayEl) {
                adminStatusOverlayEl.textContent = text || "";
                adminStatusOverlayEl.style.color = isError ? "#dc2626" : "#667085";
            }
        }

        async function switchMainView(view) {
            const isDbView = view === "db";
            if (isDbView && !state.auth?.is_superadmin) {
                setStatus("只有 SuperAdmin 可以进入数据库管理");
                return;
            }
            state.currentView = isDbView ? "db" : "chat";
            chatViewEl.classList.toggle("hidden", isDbView);
            dbViewEl.classList.toggle("hidden", !isDbView);
            chatViewBtnEl.classList.toggle("active", !isDbView);
            dbViewBtnEl.classList.toggle("active", isDbView);
            if (isDbView && !state.adminTables.length) {
                await refreshAdminTables();
            }
        }

        function escapeHtml(text) {
            return String(text || "")
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;");
        }

        function getDisplayContent(message) {
            const raw = String(message?.content || "");
            if (message?.role !== "assistant") {
                return raw;
            }
            return raw.replaceAll("#", "").replaceAll("*", "");
        }

        function formatMessageContent(message) {
            return escapeHtml(getDisplayContent(message)).replaceAll("\n", "<br>");
        }

        function getMessageImages(message) {
            if (Array.isArray(message?.images) && message.images.length) {
                return message.images.filter((image) => image?.media_type && image?.data);
            }
            if (message?.image?.media_type && message?.image?.data) {
                return [message.image];
            }
            return [];
        }

        function getMessageFiles(message) {
            if (!Array.isArray(message?.files)) {
                return [];
            }
            return message.files.filter((file) => file?.name && file?.media_type);
        }

        function getFileTypeLabel(file) {
            const mediaType = String(file?.media_type || "").toLowerCase();
            if (mediaType === "application/pdf") return "PDF";
            if (mediaType === "text/plain") return "TXT";
            if (mediaType === "application/vnd.openxmlformats-officedocument.wordprocessingml.document") return "DOCX";
            return "FILE";
        }

        function buildMessageFilesHtml(message) {
            const files = getMessageFiles(message);
            if (!files.length) {
                return "";
            }
            return `
                <div class="message-file-list">
                    ${files.map((file) => `
                        <div class="message-file-card">
                            <span class="file-badge">${getFileTypeLabel(file)}</span>
                            <div class="file-name">${escapeHtml(file.name || "附件")}</div>
                        </div>
                    `).join("")}
                </div>
            `;
        }

        function cloneFiles(files) {
            return getMessageFiles({ files }).map((file) => ({ ...file }));
        }

        function fileExtension(name) {
            const normalized = String(name || "").trim().toLowerCase();
            const dotIndex = normalized.lastIndexOf(".");
            return dotIndex >= 0 ? normalized.slice(dotIndex) : "";
        }

        function normalizeDocumentType(file) {
            const explicitType = String(file?.type || "").trim().toLowerCase();
            if (SUPPORTED_DOCUMENT_TYPES.has(explicitType)) {
                return explicitType;
            }
            if (!explicitType || explicitType === "application/octet-stream") {
                return DOCUMENT_EXTENSION_TYPES[fileExtension(file?.name)] || "";
            }
            return explicitType;
        }

        function normalizeFilesForRequest(files) {
            return (files || []).map((file) => ({
                name: file.name || null,
                media_type: file.media_type,
                data: file.data,
            }));
        }

        function isSupportedAttachmentFile(file) {
            if (!file) {
                return false;
            }
            if (SUPPORTED_IMAGE_TYPES.has(file.type)) {
                return true;
            }
            return SUPPORTED_DOCUMENT_TYPES.has(normalizeDocumentType(file));
        }

        function extractSupportedFilesFromDataTransfer(dataTransfer) {
            if (!dataTransfer) {
                return [];
            }
            const files = [
                ...Array.from(dataTransfer.files || []),
                ...Array.from(dataTransfer.items || [])
                    .filter((item) => item.kind === "file")
                    .map((item) => item.getAsFile())
                    .filter(Boolean),
            ];
            const seen = new Set();
            return files.filter((file) => {
                if (!isSupportedAttachmentFile(file)) {
                    return false;
                }
                const key = `${file.name || ""}:${file.type || ""}:${file.size || 0}:${file.lastModified || 0}`;
                if (seen.has(key)) {
                    return false;
                }
                seen.add(key);
                return true;
            });
        }

        function setComposerDragActive(active) {
            chatFormEl.classList.toggle("drag-active", Boolean(active));
        }

        async function addAttachmentsFromFiles(files, successPrefix = "已添加") {
            const added = await handleAttachmentFiles(files);
            const parts = [];
            if (added.images.length) parts.push(`图片 ${added.images.length} 张`);
            if (added.files.length) parts.push(`文件 ${added.files.length} 个`);
            setStatus(`${successPrefix}${parts.join("，")}`);
            return added;
        }

        function buildMessageImagesHtml(message) {
            const images = getMessageImages(message);
            if (!images.length) {
                return "";
            }
            return `
                <div class="message-image-grid">
                    ${images.map((image, index) => {
                        const previewSrc = buildImageDataUrl(image);
                        const alt = escapeHtml(image.name || `uploaded-image-${index + 1}`);
                        return `<div class="message-image"><img class="zoomable-image" data-preview-src="${previewSrc}" src="${previewSrc}" alt="${alt}"></div>`;
                    }).join("")}
                </div>
            `;
        }


        function bindMessageEvents(scope = messagesEl) {
            scope.querySelectorAll("[data-delete-message]").forEach((button) => {
                if (button.dataset.boundDelete === "1") return;
                button.dataset.boundDelete = "1";
                button.addEventListener("click", async () => deleteMessageFrom(Number(button.dataset.deleteMessage)));
            });
            scope.querySelectorAll("[data-edit-message]").forEach((button) => {
                if (button.dataset.boundEdit === "1") return;
                button.dataset.boundEdit = "1";
                button.addEventListener("click", async () => {
                    const target = state.messages.find((item) => item.id === Number(button.dataset.editMessage));
                    if (target) {
                        await editAndResendMessage(target);
                    }
                });
            });
            scope.querySelectorAll(".zoomable-image").forEach((image) => {
                if (image.dataset.boundZoom === "1") return;
                image.dataset.boundZoom = "1";
                image.addEventListener("click", () => openImageLightbox(image.dataset.previewSrc || image.getAttribute("src") || ""));
            });
        }

        function openImageLightbox(src) {
            if (!src || !imageLightboxEl || !imageLightboxImgEl) return;
            imageLightboxImgEl.src = src;
            imageLightboxEl.classList.remove("hidden");
        }

        function closeImageLightbox() {
            if (!imageLightboxEl || !imageLightboxImgEl) return;
            imageLightboxEl.classList.add("hidden");
            imageLightboxImgEl.removeAttribute("src");
        }

        function isMessagesNearBottom() {
            return messagesEl.scrollHeight - messagesEl.scrollTop - messagesEl.clientHeight < 72;
        }

        function buildImageDataUrl(image) {
            if (!image?.media_type || !image?.data) {
                return "";
            }
            return `data:${image.media_type};base64,${image.data}`;
        }


        function applyAuth(auth) {
            state.auth = auth;
            const loggedIn = Boolean(auth?.authenticated);
            const isSuperadmin = Boolean(auth?.is_superadmin);
            authUserEl.textContent = loggedIn ? auth.user.username : "未登录";
            authMetaEl.textContent = loggedIn
                ? `${auth.user.email} · ${auth.user.role}${auth.is_paid ? " · 付费用户" : ""}`
                : "请先登录后再开始聊天。";
            importKeysBtnEl.classList.toggle("hidden", !isSuperadmin);
            importOpus47KeysBtnEl.classList.toggle("hidden", !isSuperadmin);
            logoutBtnEl.classList.toggle("hidden", !loggedIn);
            showAuthBtnEl.classList.toggle("hidden", loggedIn);
            adminBtnEl.classList.toggle("hidden", !isSuperadmin);
            dbViewBtnEl.classList.toggle("hidden", !isSuperadmin);
            if (keyToolsEl) {
                keyToolsEl.classList.toggle("hidden", !isSuperadmin);
            }
            messageInputEl.disabled = !loggedIn || state.sending;
            imageInputEl.disabled = !loggedIn || state.sending;
            attachImageBtnEl.disabled = !loggedIn || state.sending;
            removeImageBtnEl.disabled = !loggedIn || state.sending || (!state.selectedImages.length && !state.selectedFiles.length);
            sendBtnEl.disabled = !loggedIn || state.sending;
            modelSelectEl.disabled = !loggedIn || state.sending;
            renameSessionBtnEl.disabled = !loggedIn || !state.sessionId || state.sending;
            deleteSessionBtnEl.disabled = !loggedIn || !state.sessionId || state.sending;
            renderModelNote();
            renderKeyMeta();

            if (!isSuperadmin && state.currentView === "db") {
                void switchMainView("chat");
            }

            if (!loggedIn) {
                state.currentView = "chat";
                state.sessionId = null;
                state.sessions = [];
                state.messages = [];
                state.keyCount = 0;
                state.opus47KeyCount = 0;
                clearSelectedImage();
                renderSessions();
                renderMessages();
                chatTitleEl.textContent = "新对话";
                chatSubtitleEl.textContent = "登录后历史会话会保存在 SQLite 中。";
                setStatus("请先登录");
                authOverlayEl.classList.remove("hidden");
                adminOverlayEl.classList.add("hidden");
                chatViewEl.classList.remove("hidden");
                dbViewEl.classList.add("hidden");
                chatViewBtnEl.classList.add("active");
                dbViewBtnEl.classList.remove("active");
            }
        }

        function setSending(isSending) {
            state.sending = isSending;
            applyAuth(state.auth);
            setStatus(isSending ? "正在思考..." : (state.auth?.authenticated ? "准备就绪" : "请先登录"));
        }

        function cloneImages(images) {
            return getMessageImages({ images }).map((image) => ({ ...image }));
        }

        function renderComposerImage() {
            const images = Array.isArray(state.selectedImages) ? state.selectedImages : [];
            const files = Array.isArray(state.selectedFiles) ? state.selectedFiles : [];
            state.selectedImage = images[0] || null;
            const hasAttachments = images.length || files.length;
            composerImagePreviewEl.classList.toggle("hidden", !hasAttachments);
            removeImageBtnEl.disabled = !hasAttachments || state.sending || !state.auth?.authenticated;
            if (!hasAttachments) {
                composerImageListEl.innerHTML = "";
                composerImageNameEl.textContent = "";
                return;
            }

            const imageHtml = images.map((image, index) => {
                const previewSrc = image.preview_url || buildImageDataUrl(image);
                const imageName = escapeHtml(image.name || `image-${index + 1}`);
                return `
                    <div class="composer-image-card">
                        <img class="composer-image-preview-img zoomable-image" data-preview-src="${previewSrc}" src="${previewSrc}" alt="${imageName}">
                        <div class="composer-image-card-footer">
                            <span class="helper">${imageName}</span>
                            <button class="ghost-btn composer-image-remove" type="button" data-remove-selected-image="${index}">Remove</button>
                        </div>
                    </div>
                `;
            }).join("");
            const fileHtml = files.map((file, index) => `
                <div class="composer-file-card">
                    <span class="file-badge">${getFileTypeLabel(file)}</span>
                    <div class="file-name">${escapeHtml(file.name || `attachment-${index + 1}`)}</div>
                    <div class="file-meta">会先解析文本，再把解析结果发给 AI</div>
                    <button class="ghost-btn composer-image-remove" type="button" data-remove-selected-file="${index}">Remove</button>
                </div>
            `).join("");
            composerImageListEl.innerHTML = `${imageHtml}${fileHtml}`;
            const summary = [];
            if (images.length) summary.push(`${images.length} image${images.length > 1 ? "s" : ""}`);
            if (files.length) summary.push(`${files.length} file${files.length > 1 ? "s" : ""}`);
            composerImageNameEl.textContent = `${summary.join(" + ")} selected`;
            composerImageListEl.querySelectorAll("[data-remove-selected-image]").forEach((button) => {
                button.addEventListener("click", () => {
                    const index = Number(button.dataset.removeSelectedImage);
                    if (Number.isNaN(index)) {
                        return;
                    }
                    state.selectedImages = state.selectedImages.filter((_, imageIndex) => imageIndex !== index);
                    if (!state.selectedImages.length) {
                        imageInputEl.value = "";
                    }
                    renderComposerImage();
                });
            });
            composerImageListEl.querySelectorAll("[data-remove-selected-file]").forEach((button) => {
                button.addEventListener("click", () => {
                    const index = Number(button.dataset.removeSelectedFile);
                    if (Number.isNaN(index)) {
                        return;
                    }
                    state.selectedFiles = state.selectedFiles.filter((_, fileIndex) => fileIndex !== index);
                    if (!state.selectedImages.length && !state.selectedFiles.length) {
                        imageInputEl.value = "";
                    }
                    renderComposerImage();
                });
            });
            bindMessageEvents(composerImagePreviewEl);
        }

        function clearSelectedImage() {
            state.selectedImages = [];
            state.selectedFiles = [];
            state.selectedImage = null;
            imageInputEl.value = "";
            renderComposerImage();
        }

        function normalizeImagesForRequest(images) {
            return (images || []).map((image) => ({
                name: image.name || null,
                media_type: image.media_type,
                data: image.data,
            }));
        }

        async function handleDocumentFile(file) {
            if (!file) {
                return null;
            }
            const mediaType = normalizeDocumentType(file);
            if (!SUPPORTED_DOCUMENT_TYPES.has(mediaType)) {
                throw new Error("仅支持 txt、docx、pdf 文件");
            }
            if (file.size > MAX_DOCUMENT_BYTES) {
                throw new Error("文件不能超过 10 MB");
            }

            const arrayBuffer = await file.arrayBuffer();
            const bytes = new Uint8Array(arrayBuffer);
            let binary = "";
            const chunkSize = 0x8000;
            for (let index = 0; index < bytes.length; index += chunkSize) {
                binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
            }

            const documentFile = {
                name: file.name,
                media_type: mediaType,
                data: btoa(binary),
                size: file.size,
            };
            state.selectedFiles = [...(state.selectedFiles || []), documentFile];
            renderComposerImage();
            return documentFile;
        }

        async function handleAttachmentFiles(files) {
            const addedImages = [];
            const addedFiles = [];
            for (const file of Array.from(files || [])) {
                if (SUPPORTED_IMAGE_TYPES.has(file.type)) {
                    const image = await handleImageFile(file);
                    if (image) {
                        addedImages.push(image);
                    }
                    continue;
                }
                const mediaType = normalizeDocumentType(file);
                if (SUPPORTED_DOCUMENT_TYPES.has(mediaType)) {
                    const documentFile = await handleDocumentFile(file);
                    if (documentFile) {
                        addedFiles.push(documentFile);
                    }
                    continue;
                }
                throw new Error(`不支持的文件类型：${file.name || "附件"}`);
            }
            return { images: addedImages, files: addedFiles };
        }


        function scrollMessagesToBottom() {
            messagesEl.scrollTop = messagesEl.scrollHeight;
        }

        function renderSessions() {
            if (!state.sessions.length) {
                sessionListEl.innerHTML = `<div class="helper">${state.auth?.authenticated ? "还没有历史会话，先开始一轮对话吧。" : "登录后会在这里看到你的会话。"}</div>`;
                return;
            }

            sessionListEl.innerHTML = state.sessions.map((session) => `
                <div class="session-row">
                    <button class="session-card ${session.id === state.sessionId ? "active" : ""}" type="button" data-session-id="${session.id}">
                        <div class="session-title">${escapeHtml(session.title)}</div>
                        <div class="meta-text">${escapeHtml(session.model)}</div>
                    </button>
                    <button class="danger-btn" type="button" data-delete-session="${session.id}">删</button>
                </div>
            `).join("");

            sessionListEl.querySelectorAll("[data-session-id]").forEach((button) => {
                button.addEventListener("click", async () => loadSession(button.dataset.sessionId, true));
            });
            sessionListEl.querySelectorAll("[data-delete-session]").forEach((button) => {
                button.addEventListener("click", async () => deleteSession(button.dataset.deleteSession));
            });
        }

function renderMessages() {
            if (!state.messages.length) {
                messagesEl.innerHTML = `<div class="helper">${state.auth?.authenticated ? "这里还没有消息，开始第一轮提问吧。" : "登录后即可查看和发送消息。"}</div>`;
                return;
            }

            messagesEl.innerHTML = state.messages.map((message) => buildMessageHtml(message)).join("");
            bindMessageEvents();
            scrollMessagesToBottom();
        }


        function updateLocalMessage(messageId, content) {
            state.messages = state.messages.map((message) =>
                message.id === messageId ? { ...message, content } : message
            );
            const contentEl = messagesEl.querySelector(`[data-message-content="${messageId}"]`);
            if (!contentEl) {
                renderMessages();
                return;
            }
            const shouldStickBottom = isMessagesNearBottom();
            const targetMessage = state.messages.find((item) => item.id === messageId);
            contentEl.innerHTML = targetMessage ? formatMessageContent(targetMessage) : "";
            contentEl.classList.toggle("hidden", !targetMessage?.content);
            if (shouldStickBottom) {
                scrollMessagesToBottom();
            }
        }

        function appendLocalMessage(role, content = "", images = [], files = []) {
            const messageId = uid();
            const normalizedImages = cloneImages(images);
            const normalizedFiles = cloneFiles(files);
            state.messages = [
                ...state.messages,
                {
                    id: messageId,
                    role,
                    content,
                    images: normalizedImages,
                    image: normalizedImages[0] || null,
                    files: normalizedFiles,
                },
            ];
            renderMessages();
            return messageId;
        }

        async function fetchJson(url, options = {}) {
            const response = await fetch(url, options);
            const data = await response.json().catch(() => ({}));
            if (!response.ok) {
                throw new Error(data.detail || data.error || "请求失败");
            }
            return data;
        }

        async function refreshAuth() {
            try {
                const auth = await fetchJson("/api/auth/me");
                applyAuth(auth);
                if (auth.authenticated) {
                    authOverlayEl.classList.add("hidden");
                }
                return auth;
            } catch (error) {
                applyAuth(null);
                setStatus(error.message || "认证失败");
                return null;
            }
        }

        async function refreshSessions() {
            if (!state.auth?.authenticated) {
                return;
            }
            state.sessions = await fetchJson("/api/sessions");
            renderSessions();
        }

        async function refreshKeyStatus() {
            if (!state.auth?.is_superadmin) {
                state.keyCount = 0;
                state.opus47KeyCount = 0;
                renderKeyMeta();
                return;
            }
            const data = await fetchJson("/api/keys");
            state.keyCount = data.total_keys || 0;
            state.opus47KeyCount = data.opus47_total_keys || 0;
            renderKeyMeta(data.source_url);
        }

        async function importKeysFromGithub() {
            if (!state.auth?.is_superadmin) {
                return;
            }
            state.importingKeys = true;
            renderKeyMeta();
            setStatus("正在从 GitHub 导入 Key...");
            try {
                const data = await fetchJson("/api/keys/import", {
                    method: "POST",
                });
                state.keyCount = data.total_keys || state.keyCount;
                renderKeyMeta(data.source_url);
                setStatus(`导入完成：新增 ${data.inserted_count} 个，跳过 ${data.ignored_count} 个`);
            } catch (error) {
                setStatus(error.message || "导入 Key 失败");
            } finally {
                state.importingKeys = false;
                renderKeyMeta();
            }
        }

        async function importOpus47KeysFromGithub() {
            if (!state.auth?.is_superadmin) {
                return;
            }
            state.importingOpus47Keys = true;
            renderKeyMeta();
            setStatus("Importing Opus4.7 keys from GitHub...");
            try {
                const data = await fetchJson("/api/keys/opus47/import", {
                    method: "POST",
                });
                state.opus47KeyCount = data.total_keys || state.opus47KeyCount;
                renderKeyMeta(data.source_url);
                setStatus(`Opus4.7 Key import finished: +${data.inserted_count}, skipped ${data.ignored_count}`);
            } catch (error) {
                setStatus(error.message || "Import Opus4.7 key failed");
            } finally {
                state.importingOpus47Keys = false;
                renderKeyMeta();
            }
        }

        function renderAdminUsers(users) {
            if (!users.length) {
                adminUserListEl.innerHTML = `<div class="helper">暂无用户数据</div>`;
                return;
            }

            adminUserListEl.innerHTML = users.map((user) => `
                <div class="admin-user-card">
                    <div>
                        <div class="session-title">${escapeHtml(user.username)} ${user.role === "SuperAdmin" ? "(SuperAdmin)" : ""}</div>
                        <div class="admin-user-meta">
                            ID: ${user.id}<br>
                            邮箱: ${escapeHtml(user.email)}<br>
                            状态: ${user.is_active ? "启用" : "禁用"}<br>
                            创建时间: ${escapeHtml(user.created_at || "")}
                        </div>
                    </div>
                    <button
                        class="${user.is_active ? "danger-btn" : "primary-btn"}"
                        type="button"
                        data-toggle-user="${user.id}"
                        data-next-active="${user.is_active ? "0" : "1"}"
                    >${user.is_active ? "禁用" : "启用"}</button>
                </div>
            `).join("");

            adminUserListEl.querySelectorAll("[data-toggle-user]").forEach((button) => {
                button.addEventListener("click", async () => {
                    const userId = button.dataset.toggleUser;
                    const nextActive = button.dataset.nextActive === "1";
                    await updateUserStatus(userId, nextActive);
                });
            });
        }

        async function refreshAdminUsers() {
            if (!state.auth?.is_superadmin) {
                return;
            }
            setAdminStatus("正在加载用户列表...");
            try {
                const data = await fetchJson("/api/admin/users");
                renderAdminUsers(data.users || []);
                setAdminStatus("用户列表已更新");
            } catch (error) {
                setAdminStatus(error.message || "加载用户失败", true);
            }
        }

        async function updateUserStatus(userId, isActive) {
            const actionText = isActive ? "启用" : "禁用";
            if (!window.confirm(`确定要${actionText}这个用户吗？`)) {
                return;
            }
            try {
                await fetchJson(`/api/admin/users/${userId}`, {
                    method: "PATCH",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ is_active: isActive }),
                });
                await refreshAdminUsers();
            } catch (error) {
                setAdminStatus(error.message || `${actionText}失败`, true);
            }
        }

        function renderAdminTableOptions() {
            if (!state.adminTables.length) {
                adminDbTablesEl.innerHTML = `<div class="helper">没有可管理的数据表</div>`;
                return;
            }

            adminDbTablesEl.innerHTML = state.adminTables.map((table) => `
                <button
                    class="admin-db-table-btn ${table.name === state.selectedAdminTable ? "active" : ""}"
                    type="button"
                    data-admin-table="${table.name}"
                >
                    <div>${table.name}</div>
                    <div class="helper">${table.count} 行</div>
                </button>
            `).join("");

            adminDbTablesEl.querySelectorAll("[data-admin-table]").forEach((button) => {
                button.addEventListener("click", async () => {
                    await loadAdminTable(button.dataset.adminTable);
                });
            });

        }

        function getPrimaryKeyPayload(tableInfo, row) {
            const pk = {};
            for (const key of tableInfo.primary_keys || []) {
                pk[key] = row[key];
            }
            return pk;
        }

        function getFilteredAdminRows(tableInfo) {
            const rows = tableInfo?.rows || [];
            const keyword = (state.adminDbQuery || "").trim().toLowerCase();
            if (!keyword) {
                return rows;
            }
            return rows.filter((row) =>
                Object.values(row).some((value) => String(value ?? "").toLowerCase().includes(keyword))
            );
        }

        function buildRowPromptText(tableInfo, row = {}) {
            const primaryKeys = new Set(tableInfo?.primary_keys || []);
            return (tableInfo?.columns || []).map((column) => {
                const value = row[column.name] ?? "";
                const suffix = primaryKeys.has(column.name) ? " [PK]" : "";
                return `${column.name}${suffix}=${value}`;
            }).join("\n");
        }

        function parseRowPromptText(text) {
            const values = {};
            for (const line of String(text || "").split("\n")) {
                const trimmed = line.trim();
                if (!trimmed) {
                    continue;
                }
                const separatorIndex = trimmed.indexOf("=");
                if (separatorIndex < 0) {
                    continue;
                }
                const rawKey = trimmed.slice(0, separatorIndex).trim();
                const key = rawKey.replace(/\s*\[PK\]$/, "").trim();
                const rawValue = trimmed.slice(separatorIndex + 1);
                values[key] = rawValue === "" ? null : rawValue;
            }
            return values;
        }

        function setDbModalStatus(text, isError = false) {
            dbModalStatusEl.textContent = text || "";
            dbModalStatusEl.style.color = isError ? "#dc2626" : "#667085";
        }

        function closeDbModal() {
            dbModalOverlayEl.classList.add("hidden");
            dbModalGridEl.innerHTML = "";
            state.dbModalMode = "view";
            state.dbModalRow = null;
            setDbModalStatus("");
        }

        function openDbModal(mode, row = null) {
            const tableInfo = state.selectedAdminTableInfo;
            if (!tableInfo) {
                return;
            }
            state.dbModalMode = mode;
            state.dbModalRow = row;

            const isView = mode === "view";
            const isCreate = mode === "create";
            dbModalTitleEl.textContent = isCreate ? "新增记录" : isView ? "查看记录" : "编辑记录";
            dbModalSubtitleEl.textContent = isCreate
                ? `向 ${state.selectedAdminTable} 表新增一条记录`
                : isView
                    ? `查看 ${state.selectedAdminTable} 表中的记录详情`
                    : `修改 ${state.selectedAdminTable} 表中的记录`;
            saveDbModalBtnEl.classList.toggle("hidden", isView);
            cancelDbModalBtnEl.textContent = isView ? "关闭" : "取消";
            setDbModalStatus("");

            const primaryKeys = new Set(tableInfo.primary_keys || []);
            const values = row || {};
            dbModalGridEl.innerHTML = (tableInfo.columns || []).map((column) => {
                const isPrimaryKey = primaryKeys.has(column.name);
                const isAutoPrimaryKey = isCreate && isPrimaryKey && /int/i.test(String(column.type || "")) && String(column.name || "").toLowerCase() === "id";
                if (isAutoPrimaryKey) {
                    return "";
                }
                const value = values[column.name] ?? "";
                const escapedValue = escapeHtml(String(value));
                const useTextarea = String(value).length > 80 || String(value).includes("\n");
                const readonly = isView || (!isCreate && isPrimaryKey);
                return `
                    <div class="db-modal-field">
                        <label>${column.name}${isPrimaryKey ? " [PK]" : ""}</label>
                        ${useTextarea
                            ? `<textarea data-db-modal-field="${column.name}" ${readonly ? "readonly" : ""}>${escapedValue}</textarea>`
                            : `<input data-db-modal-field="${column.name}" value="${escapedValue}" ${readonly ? "readonly" : ""}>`
                        }
                    </div>
                `;
            }).join("");

            dbModalOverlayEl.classList.remove("hidden");
        }

        function collectDbModalValues() {
            const values = {};
            dbModalGridEl.querySelectorAll("[data-db-modal-field]").forEach((fieldEl) => {
                const key = fieldEl.dataset.dbModalField;
                const rawValue = fieldEl.value;
                values[key] = rawValue === "" ? null : rawValue;
            });
            return values;
        }

        async function saveDbModal() {
            const tableInfo = state.selectedAdminTableInfo;
            if (!tableInfo) {
                return;
            }
            try {
                const values = collectDbModalValues();
                if (state.dbModalMode === "create") {
                    await fetchJson(`/api/admin/db/tables/${encodeURIComponent(state.selectedAdminTable)}/rows`, {
                        method: "POST",
                        headers: { "Content-Type": "application/json" },
                        body: JSON.stringify({ values }),
                    });
                    await loadAdminTable(state.selectedAdminTable);
                    closeDbModal();
                    setAdminStatus("新增成功");
                    return;
                }

                const currentRow = state.dbModalRow;
                if (!currentRow) {
                    setDbModalStatus("缺少待编辑记录", true);
                    return;
                }
                const pk = getPrimaryKeyPayload(tableInfo, currentRow);
                const updates = {};
                for (const [key, value] of Object.entries(values)) {
                    if (!(tableInfo.primary_keys || []).includes(key) && JSON.stringify(value) !== JSON.stringify(currentRow[key] ?? null)) {
                        updates[key] = value;
                    }
                }
                if (!Object.keys(updates).length) {
                    setDbModalStatus("没有检测到变更");
                    return;
                }
                await fetchJson(`/api/admin/db/tables/${encodeURIComponent(state.selectedAdminTable)}/rows`, {
                    method: "PATCH",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ pk, updates }),
                });
                await loadAdminTable(state.selectedAdminTable);
                closeDbModal();
                setAdminStatus("更新成功");
            } catch (error) {
                setDbModalStatus(error.message || "保存失败", true);
            }
        }

        function renderAdminTableRows(tableInfo) {
            if (!tableInfo) {
                adminDbRowsEl.innerHTML = "";
                return;
            }

            const filteredRows = getFilteredAdminRows(tableInfo);

            if (!(tableInfo.rows || []).length) {
                adminDbRowsEl.innerHTML = `<div class="empty-state">这个表目前没有数据。</div>`;
                return;
            }

            if (!filteredRows.length) {
                adminDbRowsEl.innerHTML = `<div class="empty-state">当前筛选条件下没有匹配数据。</div>`;
                return;
            }

            const columns = tableInfo.columns || [];
            adminDbRowsEl.innerHTML = `
                <div class="admin-db-table-wrap">
                    <table class="admin-db-table-grid">
                        <thead>
                            <tr>
                                <th>NO.</th>
                                ${columns.map((column) => `<th>${escapeHtml(column.name)}</th>`).join("")}
                                <th>操作</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${filteredRows.map((row, index) => `
                                <tr
                                    class="${JSON.stringify(row) === JSON.stringify(state.selectedAdminRow) ? "active" : ""}"
                                    data-db-row-index="${index}"
                                >
                                    <td>${index + 1}</td>
                                    ${columns.map((column) => `<td title="${escapeHtml(String(row[column.name] ?? ""))}">${escapeHtml(String(row[column.name] ?? ""))}</td>`).join("")}
                                    <td class="actions">
                                        <button class="table-action-btn view" type="button" data-db-view="${index}">查看</button>
                                        <button class="table-action-btn edit" type="button" data-db-edit="${index}">编辑</button>
                                        <button class="table-action-btn delete" type="button" data-db-delete="${index}">删除</button>
                                    </td>
                                </tr>
                            `).join("")}
                        </tbody>
                    </table>
                </div>
            `;

            adminDbRowsEl.querySelectorAll("[data-db-row-index]").forEach((rowEl) => {
                rowEl.addEventListener("click", () => {
                    state.selectedAdminRow = filteredRows[Number(rowEl.dataset.dbRowIndex)];
                    renderAdminTableRows(tableInfo);
                });
            });

            adminDbRowsEl.querySelectorAll("[data-db-view]").forEach((button) => {
                button.addEventListener("click", (event) => {
                    event.stopPropagation();
                    const row = filteredRows[Number(button.dataset.dbView)];
                    openDbModal("view", row);
                });
            });

            adminDbRowsEl.querySelectorAll("[data-db-edit]").forEach((button) => {
                button.addEventListener("click", async (event) => {
                    event.stopPropagation();
                    const row = filteredRows[Number(button.dataset.dbEdit)];
                    await editDbRow(row);
                });
            });

            adminDbRowsEl.querySelectorAll("[data-db-delete]").forEach((button) => {
                button.addEventListener("click", async (event) => {
                    event.stopPropagation();
                    const row = filteredRows[Number(button.dataset.dbDelete)];
                    await deleteDbRow(row);
                });
            });

            if (state.selectedAdminRow) {
                const stillExists = filteredRows.some((row) => JSON.stringify(row) === JSON.stringify(state.selectedAdminRow));
                if (!stillExists) {
                    state.selectedAdminRow = null;
                }
            }
        }

        async function refreshAdminTables() {
            if (!state.auth?.is_superadmin) {
                return;
            }
            setAdminStatus("正在加载数据库表...");
            try {
                const data = await fetchJson("/api/admin/db/tables");
                state.adminTables = data.tables || [];
                if (state.selectedAdminTable && !state.adminTables.some((table) => table.name === state.selectedAdminTable)) {
                    state.selectedAdminTable = "";
                    state.selectedAdminTableInfo = null;
                    state.selectedAdminRow = null;
                }
                if (!state.selectedAdminTable && state.adminTables.length) {
                    state.selectedAdminTable = state.adminTables[0].name;
                }
                renderAdminTableOptions();
                if (state.selectedAdminTable) {
                    await loadAdminTable(state.selectedAdminTable);
                } else {
                    renderAdminTableRows(null);
                    setAdminStatus("没有可管理的数据表");
                }
            } catch (error) {
                setAdminStatus(error.message || "加载数据库表失败", true);
            }
        }

        async function loadAdminTable(tableName) {
            if (!tableName) {
                state.selectedAdminTable = "";
                state.selectedAdminTableInfo = null;
                renderAdminTableRows(null);
                return;
            }
            state.selectedAdminTable = tableName;
            dbSearchInputEl.value = state.adminDbQuery;
            renderAdminTableOptions();
            try {
                const data = await fetchJson(`/api/admin/db/tables/${encodeURIComponent(tableName)}?limit=50&offset=0`);
                state.selectedAdminTableInfo = data;
                state.selectedAdminRow = null;
                renderAdminTableRows(data);
                setAdminStatus(`已加载数据表 ${tableName}`);
            } catch (error) {
                setAdminStatus(error.message || "加载表数据失败", true);
            }
        }

        async function insertDbRow() {
            if (!state.selectedAdminTable) {
                setAdminStatus("请先选择一个数据表", true);
                return;
            }
            try {
                openDbModal("create");
            } catch (error) {
                setAdminStatus(error.message || "新增行失败", true);
            }
        }

        async function editDbRow(row) {
            const tableInfo = state.selectedAdminTableInfo;
            if (!tableInfo) {
                return;
            }
            try {
                openDbModal("edit", row);
            } catch (error) {
                setAdminStatus(error.message || "更新失败", true);
            }
        }

        function applyDbSearch() {
            state.adminDbQuery = dbSearchInputEl.value.trim();
            renderAdminTableRows(state.selectedAdminTableInfo);
        }

        async function deleteDbRow(row) {
            const tableInfo = state.selectedAdminTableInfo;
            if (!tableInfo) {
                return;
            }
            if (!window.confirm(`确定删除 ${state.selectedAdminTable} 表中的这条记录吗？`)) {
                return;
            }
            try {
                const pk = getPrimaryKeyPayload(tableInfo, row);
                await fetchJson(`/api/admin/db/tables/${encodeURIComponent(state.selectedAdminTable)}/rows`, {
                    method: "DELETE",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ pk }),
                });
                state.selectedAdminRow = null;
                await loadAdminTable(state.selectedAdminTable);
            } catch (error) {
                setAdminStatus(error.message || "删除行失败", true);
            }
        }

        async function createSession() {
            if (!state.auth?.authenticated) {
                authOverlayEl.classList.remove("hidden");
                return;
            }
            const session = await fetchJson("/api/sessions", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ model: modelSelectEl.value }),
            });
            state.messages = [];
            state.sessionId = session.id;
            chatTitleEl.textContent = session.title;
            chatSubtitleEl.textContent = session.id;
            renderMessages();
            await refreshSessions();
        }

        async function loadSession(sessionId, syncModel = false) {
            const data = await fetchJson(`/api/sessions/${sessionId}`);
            state.sessionId = data.session.id;
            state.messages = data.messages || [];
            chatTitleEl.textContent = data.session.title;
            chatSubtitleEl.textContent = data.session.id;
            if (syncModel) {
                modelSelectEl.value = data.session.model;
            }
            applyAuth(state.auth);
            renderMessages();
            renderSessions();
        }

        async function renameSession(sessionId = state.sessionId) {
            if (!sessionId) return;
            const target = state.sessions.find((item) => item.id === sessionId);
            if (!target) return;
            const title = window.prompt("输入新的会话标题", target.title);
            if (title === null) return;
            const trimmed = title.trim();
            if (!trimmed) {
                setStatus("标题不能为空");
                return;
            }
            const data = await fetchJson(`/api/sessions/${sessionId}`, {
                method: "PATCH",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ title: trimmed }),
            });
            chatTitleEl.textContent = data.session.title;
            await refreshSessions();
            setStatus("会话标题已更新");
        }

        async function deleteSession(sessionId = state.sessionId) {
            if (!sessionId) return;
            const target = state.sessions.find((item) => item.id === sessionId);
            if (!target) return;
            if (!window.confirm(`确定删除会话「${target.title}」吗？`)) return;
            await fetchJson(`/api/sessions/${sessionId}`, { method: "DELETE" });
            if (state.sessionId === sessionId) {
                state.sessionId = null;
                state.messages = [];
                renderMessages();
                chatTitleEl.textContent = "新对话";
                chatSubtitleEl.textContent = "当前没有打开的会话";
            }
            await refreshSessions();
            if (!state.sessionId && state.sessions.length) {
                await loadSession(state.sessions[0].id, true);
            }
            setStatus("会话已删除");
        }

        async function deleteMessageFrom(messageId) {
            if (!state.sessionId) return;
            if (!window.confirm("确定从这条消息开始删除吗？这条及后续消息都会被删除。")) return;
            setSending(true);
            try {
                const data = await fetchJson(`/api/sessions/${state.sessionId}/messages/${messageId}`, {
                    method: "DELETE",
                });
                state.messages = data.messages || [];
                chatTitleEl.textContent = data.session.title;
                renderMessages();
                await refreshSessions();
                setStatus("消息已删除");
            } catch (error) {
                setStatus(error.message || "删除失败");
            } finally {
                setSending(false);
            }
        }



        function switchAuthTab(tab) {
            document.querySelectorAll(".tab-btn").forEach((button) => {
                button.classList.toggle("active", button.dataset.tab === tab);
            });
            document.querySelectorAll(".auth-form").forEach((form) => {
                form.classList.toggle("hidden", form.id !== `${tab}-form`);
            });
            setAuthStatus("");
        }

        async function submitJsonForm(url, payload, successMessage) {
            const data = await fetchJson(url, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });
            setAuthStatus(successMessage || data.message || "操作成功");
            return data;
        }

        document.querySelectorAll(".tab-btn").forEach((button) => {
            button.addEventListener("click", () => switchAuthTab(button.dataset.tab));
        });

        chatViewBtnEl.addEventListener("click", async () => {
            await switchMainView("chat");
        });

        dbViewBtnEl.addEventListener("click", async () => {
            await switchMainView("db");
        });

        showAuthBtnEl.addEventListener("click", () => {
            authOverlayEl.classList.remove("hidden");
        });

        adminBtnEl.addEventListener("click", async () => {
            adminOverlayEl.classList.remove("hidden");
            await refreshAdminUsers();
        });

        document.getElementById("close-auth-btn").addEventListener("click", () => {
            if (!state.auth?.authenticated) return;
            authOverlayEl.classList.add("hidden");
        });

        document.getElementById("close-admin-btn").addEventListener("click", () => {
            adminOverlayEl.classList.add("hidden");
        });

        document.getElementById("image-lightbox-close").addEventListener("click", () => {
            closeImageLightbox();
        });

        imageLightboxEl.addEventListener("click", (event) => {
            if (event.target === imageLightboxEl) {
                closeImageLightbox();
            }
        });

        document.addEventListener("keydown", (event) => {
            if (event.key === "Escape" && !imageLightboxEl.classList.contains("hidden")) {
                closeImageLightbox();
            }
        });

        composerImagePreviewEl.addEventListener("click", (event) => {
            const target = event.target.closest(".zoomable-image");
            if (!target) {
                return;
            }
            openImageLightbox(target.dataset.previewSrc || target.getAttribute("src") || "");
        });

        document.getElementById("refresh-admin-btn").addEventListener("click", async () => {
            await refreshAdminUsers();
        });

        dbRefreshBtnEl.addEventListener("click", async () => {
            await refreshAdminTables();
        });

        dbBackChatBtnEl.addEventListener("click", async () => {
            await switchMainView("chat");
        });

        dbSearchBtnEl.addEventListener("click", () => {
            applyDbSearch();
        });

        dbClearSearchBtnEl.addEventListener("click", () => {
            dbSearchInputEl.value = "";
            state.adminDbQuery = "";
            renderAdminTableRows(state.selectedAdminTableInfo);
        });

        closeDbModalBtnEl.addEventListener("click", () => {
            closeDbModal();
        });

        cancelDbModalBtnEl.addEventListener("click", () => {
            closeDbModal();
        });

        saveDbModalBtnEl.addEventListener("click", async () => {
            await saveDbModal();
        });

        dbModalOverlayEl.addEventListener("click", (event) => {
            if (event.target === dbModalOverlayEl) {
                closeDbModal();
            }
        });

        dbSearchInputEl.addEventListener("keydown", (event) => {
            if (event.key === "Enter") {
                event.preventDefault();
                applyDbSearch();
            }
        });

        document.getElementById("refresh-db-btn").addEventListener("click", async () => {
            await refreshAdminTables();
        });

        document.getElementById("refresh-db-rows-btn").addEventListener("click", async () => {
            await loadAdminTable(state.selectedAdminTable);
        });

        document.getElementById("insert-db-row-btn").addEventListener("click", async () => {
            await insertDbRow();
        });

        document.getElementById("new-chat-btn").addEventListener("click", async () => {
            try {
                await createSession();
                messageInputEl.focus();
            } catch (error) {
                setStatus(error.message || "创建会话失败");
            }
        });

        attachImageBtnEl.addEventListener("click", () => {
            if (attachImageBtnEl.disabled) {
                return;
            }
            imageInputEl.click();
        });

        imageInputEl.addEventListener("change", async (event) => {
            const [file] = event.target.files || [];
            if (!file) {
                return;
            }
            try {
                await handleImageFile(file);
                setStatus(`已选择图片：${file.name}`);
            } catch (error) {
                clearSelectedImage();
                setStatus(error.message || "选择图片失败");
            }
        });

        messageInputEl.addEventListener("paste", async (event) => {
            const items = Array.from(event.clipboardData?.items || []);
            const imageItem = items.find((item) => item.type && item.type.startsWith("image/"));
            if (!imageItem) {
                return;
            }
            const file = imageItem.getAsFile();
            if (!file) {
                return;
            }
            event.preventDefault();
            try {
                await handleImageFile(file);
                setStatus(`已粘贴图片：${file.name || "clipboard-image"}`);
            } catch (error) {
                clearSelectedImage();
                setStatus(error.message || "粘贴图片失败");
            }
        });

        removeImageBtnEl.addEventListener("click", () => {
            clearSelectedImage();
            setStatus(state.auth?.authenticated ? "已移除图片" : "请先登录");
        });

        logoutBtnEl.addEventListener("click", async () => {
            try {
                await submitJsonForm("/api/auth/logout", {}, "已退出登录");
                await refreshAuth();
            } catch (error) {
                setStatus(error.message || "退出失败");
            }
        });

        renameSessionBtnEl.addEventListener("click", async () => {
            try {
                await renameSession();
            } catch (error) {
                setStatus(error.message || "重命名失败");
            }
        });

        deleteSessionBtnEl.addEventListener("click", async () => {
            try {
                await deleteSession();
            } catch (error) {
                setStatus(error.message || "删除失败");
            }
        });

        importKeysBtnEl.addEventListener("click", async () => {
            await importKeysFromGithub();
        });

        importOpus47KeysBtnEl.addEventListener("click", async () => {
            await importOpus47KeysFromGithub();
        });

        document.getElementById("chat-form").addEventListener("submit", async (event) => {
            event.preventDefault();
            await sendMessage();
        });

        messageInputEl.addEventListener("keydown", async (event) => {
            if (event.key === "Enter" && event.ctrlKey) {
                event.preventDefault();
                await sendMessage();
            }
        });

        document.getElementById("send-register-code-btn").addEventListener("click", async () => {
            const form = document.getElementById("register-form");
            const email = form.email.value.trim();
            if (!email) {
                setAuthStatus("请先填写邮箱", true);
                return;
            }
            try {
                await submitJsonForm("/api/auth/register-verify", { email }, "注册验证码已发送");
            } catch (error) {
                setAuthStatus(error.message || "发送失败", true);
            }
        });

        document.getElementById("send-reset-code-btn").addEventListener("click", async () => {
            const form = document.getElementById("reset-form");
            const email = form.email.value.trim();
            if (!email) {
                setAuthStatus("请先填写邮箱", true);
                return;
            }
            try {
                await submitJsonForm("/api/auth/verify", { email }, "重置验证码已发送");
            } catch (error) {
                setAuthStatus(error.message || "发送失败", true);
            }
        });

        document.getElementById("login-form").addEventListener("submit", async (event) => {
            event.preventDefault();
            const form = event.currentTarget;
            try {
                await submitJsonForm("/api/auth/login", {
                    username: form.username.value.trim(),
                    password: form.password.value,
                }, "登录成功");
                form.reset();
                await refreshAuth();
                await refreshSessions();
                if (state.sessions.length) {
                    await loadSession(state.sessions[0].id, true);
                }
            } catch (error) {
                setAuthStatus(error.message || "登录失败", true);
            }
        });

        document.getElementById("register-form").addEventListener("submit", async (event) => {
            event.preventDefault();
            const form = event.currentTarget;
            try {
                await submitJsonForm("/api/auth/register", {
                    username: form.username.value.trim(),
                    email: form.email.value.trim(),
                    verifyCode: form.verifyCode.value.trim(),
                    password: form.password.value,
                    confirmPassword: form.confirmPassword.value,
                }, "注册成功，请直接登录");
                form.reset();
                switchAuthTab("login");
            } catch (error) {
                setAuthStatus(error.message || "注册失败", true);
            }
        });

        document.getElementById("reset-form").addEventListener("submit", async (event) => {
            event.preventDefault();
            const form = event.currentTarget;
            try {
                await submitJsonForm("/api/auth/reset", {
                    email: form.email.value.trim(),
                    verifyCode: form.verifyCode.value.trim(),
                    password: form.password.value,
                    confirmPassword: form.confirmPassword.value,
                }, "密码已重置，请重新登录");
                form.reset();
                switchAuthTab("login");
            } catch (error) {
                setAuthStatus(error.message || "重置失败", true);
            }
        });

        imageInputEl.addEventListener("change", async (event) => {
            event.stopImmediatePropagation();
            const files = Array.from(event.target.files || []);
            if (!files.length) {
                return;
            }
            try {
                await addAttachmentsFromFiles(files, "已添加");
            } catch (error) {
                clearSelectedImage();
                setStatus(error.message || "选择附件失败");
            }
        }, true);

        messageInputEl.addEventListener("paste", async (event) => {
            const files = extractSupportedFilesFromDataTransfer(event.clipboardData);
            if (!files.length) {
                return;
            }
            event.preventDefault();
            event.stopImmediatePropagation();
            try {
                await addAttachmentsFromFiles(files, "已粘贴");
            } catch (error) {
                clearSelectedImage();
                setStatus(error.message || "粘贴附件失败");
            }
        }, true);

        chatFormEl.addEventListener("dragenter", (event) => {
            const files = extractSupportedFilesFromDataTransfer(event.dataTransfer);
            if (!files.length || !state.auth?.authenticated || state.sending) {
                return;
            }
            event.preventDefault();
            composerDragDepth += 1;
            setComposerDragActive(true);
        });

        chatFormEl.addEventListener("dragover", (event) => {
            const files = extractSupportedFilesFromDataTransfer(event.dataTransfer);
            if (!files.length || !state.auth?.authenticated || state.sending) {
                return;
            }
            event.preventDefault();
            if (event.dataTransfer) {
                event.dataTransfer.dropEffect = "copy";
            }
            setComposerDragActive(true);
        });

        chatFormEl.addEventListener("dragleave", (event) => {
            if (!chatFormEl.classList.contains("drag-active")) {
                return;
            }
            if (event.relatedTarget && chatFormEl.contains(event.relatedTarget)) {
                return;
            }
            composerDragDepth = Math.max(0, composerDragDepth - 1);
            if (composerDragDepth === 0) {
                setComposerDragActive(false);
            }
        });

        chatFormEl.addEventListener("drop", async (event) => {
            const files = extractSupportedFilesFromDataTransfer(event.dataTransfer);
            composerDragDepth = 0;
            setComposerDragActive(false);
            if (!files.length || !state.auth?.authenticated || state.sending) {
                return;
            }
            event.preventDefault();
            event.stopPropagation();
            try {
                await addAttachmentsFromFiles(files, "已拖入");
            } catch (error) {
                clearSelectedImage();
                setStatus(error.message || "拖拽附件失败");
            }
        });

        function buildMessageHtml(message) {
            const images = getMessageImages(message);
            const files = getMessageFiles(message);
            return `
                <div class="message ${message.role}" data-message-id="${message.id}">
                    <div class="message-top">
                        <div class="message-role">${message.role === "user" ? "用户" : "AI"}</div>
                        <div class="message-actions">
                            ${message.role === "user" && !images.length && !files.length ? `<button class="message-btn" type="button" data-edit-message="${message.id}">编辑后重发</button>` : ""}
                            <button class="message-btn danger" type="button" data-delete-message="${message.id}">从此删除</button>
                        </div>
                    </div>
                    ${buildMessageImagesHtml({ images })}
                    ${buildMessageFilesHtml({ files })}
                    <div class="message-content ${message.content ? "" : "hidden"}" data-message-content="${message.id}">${message.content ? formatMessageContent(message) : ""}</div>
                </div>
            `;
        }

        async function handleImageFile(file) {
            if (!file) {
                return null;
            }
            if (!SUPPORTED_IMAGE_TYPES.has(file.type)) {
                throw new Error("仅支持 JPG、PNG、WEBP、GIF 图片");
            }
            if (file.size > MAX_IMAGE_BYTES) {
                throw new Error("图片不能超过 5 MB");
            }

            const dataUrl = await new Promise((resolve, reject) => {
                const reader = new FileReader();
                reader.onload = () => resolve(reader.result);
                reader.onerror = () => reject(new Error("读取图片失败"));
                reader.readAsDataURL(file);
            });

            const dataUrlText = String(dataUrl || "");
            const commaIndex = dataUrlText.indexOf(",");
            if (commaIndex < 0) {
                throw new Error("图片编码失败");
            }

            const image = {
                name: file.name,
                media_type: file.type,
                data: dataUrlText.slice(commaIndex + 1),
                preview_url: dataUrlText,
            };
            state.selectedImages = [...(state.selectedImages || []), image];
            state.selectedImage = state.selectedImages[0] || image;
            renderComposerImage();
            return image;
        }

        async function editAndResendMessage(message) {
            if (getMessageImages(message).length || getMessageFiles(message).length) {
                setStatus("带附件的消息暂不支持编辑后重发");
                return;
            }
            const edited = window.prompt("修改后会从这里重新生成后续对话。", message.content);
            if (edited === null) return;

            const trimmed = edited.trim();
            if (!trimmed) {
                setStatus("消息不能为空");
                return;
            }

            const targetIndex = state.messages.findIndex((item) => item.id === message.id);
            if (targetIndex < 0) {
                setStatus("未找到要编辑的消息");
                return;
            }

            const previousMessages = state.messages.map((item) => {
                const images = cloneImages(getMessageImages(item));
                const files = cloneFiles(getMessageFiles(item));
                return {
                    ...item,
                    images,
                    image: images[0] || null,
                    files,
                };
            });

            const optimisticAssistantId = uid();
            state.messages = [
                ...state.messages.slice(0, targetIndex),
                { ...message, content: trimmed, images: [], image: null, files: [] },
                { id: optimisticAssistantId, role: "assistant", content: UPSTREAM_WAITING_TEXT, images: [], image: null, files: [] },
            ];
            state.currentAssistantMessageId = optimisticAssistantId;
            renderMessages();
            setSending(true);
            setStatus(UPSTREAM_WAITING_TEXT);

            try {
                const response = await fetch("/api/chat/stream", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        session_id: state.sessionId,
                        message: trimmed,
                        model: modelSelectEl.value,
                        replace_from_message_id: message.id,
                    }),
                });

                if (!response.ok) {
                    const data = await response.json().catch(() => ({}));
                    throw new Error(data.detail || "重发失败");
                }

                const reader = response.body.getReader();
                const decoder = new TextDecoder("utf-8");
                let buffer = "";
                let assistantText = "";

                while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    buffer += decoder.decode(value, { stream: true });
                    const chunks = buffer.split("\n\n");
                    buffer = chunks.pop() || "";

                    for (const chunk of chunks) {
                        const lines = chunk.split("\n");
                        let eventName = "message";
                        let dataText = "";

                        for (const line of lines) {
                            if (line.startsWith("event:")) eventName = line.slice(6).trim();
                            if (line.startsWith("data:")) dataText += line.slice(5).trim();
                        }

                        if (!dataText) continue;
                        const payload = JSON.parse(dataText);

                        if (eventName === "start" && payload.session) {
                            state.sessionId = payload.session.id;
                            chatTitleEl.textContent = payload.session.title;
                            chatSubtitleEl.textContent = payload.session.id;
                            updateLocalMessage(state.currentAssistantMessageId, UPSTREAM_CONNECTED_TEXT);
                            setStatus(UPSTREAM_CONNECTED_TEXT);
                        }
                        if (eventName === "delta") {
                            assistantText += payload.text || "";
                            updateLocalMessage(state.currentAssistantMessageId, assistantText);
                        }
                        if (eventName === "done") {
                            assistantText = payload.reply || assistantText;
                            updateLocalMessage(state.currentAssistantMessageId, assistantText);
                            if (payload.session) {
                                chatTitleEl.textContent = payload.session.title;
                                chatSubtitleEl.textContent = payload.session.id;
                            }
                            setStatus("已重新生成后续对话");
                            refreshSessions().catch(() => {});
                            loadSession(state.sessionId).catch(() => {});
                        }
                        if (eventName === "error") {
                            throw new Error(payload.detail || "重发失败");
                        }
                    }
                }
            } catch (error) {
                state.messages = previousMessages;
                renderMessages();
                setStatus(error.message || "重发失败");
            } finally {
                state.currentAssistantMessageId = null;
                setSending(false);
            }
        }

        async function sendMessage() {
            const message = messageInputEl.value.trim();
            const selectedImages = cloneImages(state.selectedImages || []);
            const selectedFiles = cloneFiles(state.selectedFiles || []);
            const images = normalizeImagesForRequest(selectedImages);
            const files = normalizeFilesForRequest(selectedFiles);
            if ((!message && !images.length && !files.length) || state.sending) return;
            if (!state.sessionId) {
                try {
                    await createSession();
                } catch (error) {
                    setStatus(error.message || "创建会话失败");
                    return;
                }
            }
            if (!state.sessionId) return;

            const pendingMessage = message;
            const pendingImages = images;
            const pendingFiles = files;
            messageInputEl.value = "";
            clearSelectedImage();
            appendLocalMessage("user", pendingMessage, pendingImages, pendingFiles);
            state.currentAssistantMessageId = appendLocalMessage("assistant", UPSTREAM_WAITING_TEXT);
            setSending(true);
            setStatus(UPSTREAM_WAITING_TEXT);

            try {
                const response = await fetch("/api/chat/stream", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        session_id: state.sessionId,
                        message: pendingMessage,
                        images: pendingImages,
                        files: pendingFiles,
                        model: modelSelectEl.value,
                    }),
                });

                if (!response.ok) {
                    const data = await response.json().catch(() => ({}));
                    throw new Error(data.detail || "发送失败");
                }

                const reader = response.body.getReader();
                const decoder = new TextDecoder("utf-8");
                let buffer = "";
                let assistantText = "";

                while (true) {
                    const { value, done } = await reader.read();
                    if (done) break;
                    buffer += decoder.decode(value, { stream: true });
                    const chunks = buffer.split("\n\n");
                    buffer = chunks.pop() || "";

                    for (const chunk of chunks) {
                        const lines = chunk.split("\n");
                        let eventName = "message";
                        let dataText = "";

                        for (const line of lines) {
                            if (line.startsWith("event:")) eventName = line.slice(6).trim();
                            if (line.startsWith("data:")) dataText += line.slice(5).trim();
                        }

                        if (!dataText) continue;
                        const payload = JSON.parse(dataText);

                        if (eventName === "start" && payload.session) {
                            state.sessionId = payload.session.id;
                            chatTitleEl.textContent = payload.session.title;
                            chatSubtitleEl.textContent = payload.session.id;
                            updateLocalMessage(state.currentAssistantMessageId, UPSTREAM_CONNECTED_TEXT);
                            setStatus(UPSTREAM_CONNECTED_TEXT);
                        }
                        if (eventName === "delta") {
                            assistantText += payload.text || "";
                            updateLocalMessage(state.currentAssistantMessageId, assistantText);
                        }
                        if (eventName === "done") {
                            assistantText = payload.reply || assistantText;
                            updateLocalMessage(state.currentAssistantMessageId, assistantText);
                            if (payload.session) {
                                chatTitleEl.textContent = payload.session.title;
                                chatSubtitleEl.textContent = payload.session.id;
                            }
                            await refreshSessions();
                        }
                        if (eventName === "error") {
                            throw new Error(payload.detail || "发送失败");
                        }
                    }
                }
            } catch (error) {
                state.messages = state.messages.slice(0, -2);
                state.selectedImages = selectedImages;
                state.selectedFiles = selectedFiles;
                state.selectedImage = selectedImages[0] || null;
                renderComposerImage();
                renderMessages();
                setStatus(error.message || "发送失败");
            } finally {
                state.currentAssistantMessageId = null;
                setSending(false);
                messageInputEl.focus();
            }
        }

        async function init() {
            renderKeyMeta();
            renderSessions();
            renderMessages();
            renderComposerImage();
            const auth = await refreshAuth();
            if (auth?.authenticated) {
                if (auth.is_superadmin) {
                    await refreshKeyStatus();
                }
                await refreshSessions();
                if (state.sessions.length) {
                    await loadSession(state.sessions[0].id, true);
                } else {
                    setStatus("准备就绪");
                }
            }
        }

        init();
    