const $ = (id) => document.getElementById(id);

const inputs = {
  domainFilter: $("domainFilter"),
  wsEndpoint: $("wsEndpoint"),
  restEndpoint: $("restEndpoint"),
  restAllowlist: $("restAllowlist")
};

const statusEl = $("status");

function toStatusText(status) {
  return [
    `Forwarding: ${status.active ? "ON" : "OFF"}`,
    `Attached tab: ${status.attachedTabId ?? "-"}`,
    `Domain filter: ${status.config.domainFilter}`,
    `WS socket (${status.config.wsEndpoint}): ${status.sockets.websocket}`,
    `REST socket (${status.config.restEndpoint}): ${status.sockets.rest}`,
    `Command socket (${status.config.commandEndpoint}): ${status.sockets.command}`,
    `REST allowlist entries: ${(status.config.restAllowlist || []).length}`,
    `Recording: ${status.recording ? "ON" : "OFF"} (${status.recordCount ?? 0} entries)`,
    `Last error: ${status.lastError || "-"}`
  ].join("\n");
}

function updateFormFromStatus(status) {
  inputs.domainFilter.value = status.config.domainFilter || "";
  inputs.wsEndpoint.value = status.config.wsEndpoint || "";
  inputs.restEndpoint.value = status.config.restEndpoint || "";
  inputs.restAllowlist.value = (status.config.restAllowlist || []).join("\n");
}

function updateStatus(status) {
  statusEl.textContent = toStatusText(status);
}

async function send(action, payload = {}) {
  const response = await chrome.runtime.sendMessage({
    action,
    ...payload
  });
  if (!response?.ok) {
    throw new Error(response?.error || "Unknown extension error");
  }
  return response.status;
}

function readConfig() {
  const restAllowlist = inputs.restAllowlist.value
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0);

  return {
    domainFilter: inputs.domainFilter.value.trim(),
    wsEndpoint: inputs.wsEndpoint.value.trim(),
    restEndpoint: inputs.restEndpoint.value.trim(),
    restAllowlist
  };
}

async function refreshStatus() {
  const status = await send("getStatus");
  updateFormFromStatus(status);
  updateStatus(status);
}

$("saveConfig").addEventListener("click", async () => {
  try {
    const status = await send("updateConfig", { config: readConfig() });
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Save failed: ${error.message}`;
  }
});

$("start").addEventListener("click", async () => {
  try {
    await send("updateConfig", { config: readConfig() });
    const status = await send("start");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Start failed: ${error.message}`;
  }
});

$("stop").addEventListener("click", async () => {
  try {
    const status = await send("stop");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Stop failed: ${error.message}`;
  }
});

$("recStart").addEventListener("click", async () => {
  try {
    const status = await send("startRecord");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Record start failed: ${error.message}`;
  }
});

$("recStop").addEventListener("click", async () => {
  try {
    const status = await send("stopRecord");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Record stop failed: ${error.message}`;
  }
});

$("recExport").addEventListener("click", async () => {
  try {
    const response = await chrome.runtime.sendMessage({ action: "getRecording" });
    if (!response?.ok) {
      throw new Error(response?.error || "Unknown error");
    }
    const lines = (response.recording || [])
      .map((entry) => JSON.stringify(entry))
      .join("\n");
    const blob = new Blob([lines], { type: "application/x-ndjson" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `rfq_recording_${Date.now()}.jsonl`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
    statusEl.textContent = `Exported ${(response.recording || []).length} entries.`;
  } catch (error) {
    statusEl.textContent = `Export failed: ${error.message}`;
  }
});

$("recClear").addEventListener("click", async () => {
  try {
    const status = await send("clearRecording");
    updateStatus(status);
  } catch (error) {
    statusEl.textContent = `Clear failed: ${error.message}`;
  }
});

chrome.runtime.onMessage.addListener((message) => {
  if (message.event === "status" && message.status) {
    updateStatus(message.status);
  }
});

refreshStatus().catch((error) => {
  statusEl.textContent = `Failed to load status: ${error.message}`;
});
