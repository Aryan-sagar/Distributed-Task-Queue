DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Task Queue Broker — Dashboard</title>
<style>
  :root {
    --bg: #0f1115;
    --card: #171a21;
    --border: #262b36;
    --text: #e6e9ef;
    --muted: #8b93a7;
    --idle: #6b7280;
    --busy: #22c55e;
    --stopped: #ef4444;
    --unknown: #f59e0b;
  }
  * { box-sizing: border-box; }
  body {
    margin: 0;
    background: var(--bg);
    color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    padding: 32px;
  }
  h1 { font-size: 18px; font-weight: 600; margin: 0 0 24px; color: var(--muted); }
  .stats { display: flex; gap: 16px; margin-bottom: 32px; flex-wrap: wrap; }
  .stat {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 16px 24px;
    min-width: 140px;
  }
  .stat .value { font-size: 28px; font-weight: 700; }
  .stat .label { font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; }
  .workers { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px, 1fr)); gap: 12px; }
  .worker {
    background: var(--card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 12px 16px;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .dot { width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }
  .dot.idle { background: var(--idle); }
  .dot.busy { background: var(--busy); box-shadow: 0 0 8px var(--busy); }
  .dot.stopped { background: var(--stopped); }
  .dot.unknown { background: var(--unknown); }
  .worker-id { font-weight: 600; font-size: 14px; }
  .worker-task { font-size: 12px; color: var(--muted); margin-top: 2px; }
  #conn-status { font-size: 12px; color: var(--muted); margin-bottom: 8px; }
</style>
</head>
<body>
  <div id="conn-status">connecting…</div>
  <h1>Task Queue Broker</h1>

  <div class="stats">
    <div class="stat"><div class="value" id="queue-depth">–</div><div class="label">Pending</div></div>
    <div class="stat"><div class="value" id="delayed-count">–</div><div class="label">Retrying</div></div>
    <div class="stat"><div class="value" id="dlq-count">–</div><div class="label">Dead-lettered</div></div>
  </div>

  <div class="workers" id="workers"></div>

<script>
  const connStatusEl = document.getElementById("conn-status");
  const queueDepthEl = document.getElementById("queue-depth");
  const delayedCountEl = document.getElementById("delayed-count");
  const dlqCountEl = document.getElementById("dlq-count");
  const workersEl = document.getElementById("workers");

  function render(snapshot) {
    queueDepthEl.textContent = snapshot.queue_depth;
    delayedCountEl.textContent = snapshot.delayed_count;
    dlqCountEl.textContent = snapshot.dlq_count;

    workersEl.innerHTML = "";
    for (const w of snapshot.workers) {
      const card = document.createElement("div");
      card.className = "worker";
      const dot = document.createElement("div");
      dot.className = "dot " + w.status;
      const info = document.createElement("div");
      const idLine = document.createElement("div");
      idLine.className = "worker-id";
      idLine.textContent = w.worker_id;
      const taskLine = document.createElement("div");
      taskLine.className = "worker-task";
      taskLine.textContent = w.current_task_id ? ("task " + w.current_task_id.slice(0, 8)) : w.status;
      info.appendChild(idLine);
      info.appendChild(taskLine);
      card.appendChild(dot);
      card.appendChild(info);
      workersEl.appendChild(card);
    }
  }

  function connect() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const ws = new WebSocket(proto + "://" + location.host + "/ws/dashboard");

    ws.onopen = () => { connStatusEl.textContent = "connected"; };
    ws.onmessage = (event) => render(JSON.parse(event.data));
    ws.onclose = () => {
      connStatusEl.textContent = "disconnected — retrying…";
      setTimeout(connect, 2000);
    };
    ws.onerror = () => ws.close();
  }

  connect();
</script>
</body>
</html>
"""
