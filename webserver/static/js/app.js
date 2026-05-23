const form  = document.getElementById('job-form');
const flash = document.getElementById('flash');
const btn   = document.getElementById('submit-btn');
const area  = document.getElementById('jobs-area');

function showFlash(msg, ok) {
  flash.textContent = msg;
  flash.className = ok ? 'ok' : 'err';
  setTimeout(() => { flash.style.display = 'none'; }, 4000);
}

form.addEventListener('submit', async (e) => {
  e.preventDefault();
  const ds = form.querySelector('input[name=dataset]:checked');
  const payload = {
    engine: 'hadoop',
    dataset_path:    ds.dataset.path,
    dataset_size_mb: parseInt(ds.dataset.size, 10),
    window_size:     parseInt(form.window_size.value, 10),
    num_reducers:    parseInt(form.num_reducers.value, 10),
  };
  btn.disabled = true;
  try {
    const r = await fetch('/api/jobs/', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify(payload),
    });
    const data = await r.json();
    if (r.ok) {
      showFlash(`Pipeline #${data.id} submitted on ${ds.value} corpus`, true);
      refreshJobs();
    } else {
      showFlash('Validation error: ' + JSON.stringify(data), false);
    }
  } catch (err) {
    showFlash('Network error: ' + err.message, false);
  } finally {
    btn.disabled = false;
  }
});

function fmtDuration(s) {
  if (s == null) return '—';
  if (s < 60) return s.toFixed(1) + 's';
  const m = Math.floor(s / 60), r = Math.round(s % 60);
  return `${m}m ${r}s`;
}

function shortPath(p) {
  if (!p) return '—';
  const parts = p.split('/').filter(Boolean);
  return parts[parts.length - 1] || p;
}

function renderProgress(live) {
  if (!live) return '—';
  const pp    = live.pipeline_progress;
  const ypct  = live.progress != null ? live.progress : 0;
  const ppct  = pp != null ? pp : ypct;
  const stage = live.stage_index ? `${live.stage_index}/${live.stage_total}` : '?';
  const name  = live.stage_name || '';
  return `
    <div class="progress">
      <div class="progress-bar"><span style="width:${ppct}%"></span></div>
      <div class="progress-label">
        <span class="stage-pill">${stage}</span>
        <span class="stage-name">${name}</span>
      </div>
    </div>`;
}

async function refreshJobs() {
  try {
    const r = await fetch('/api/jobs/');
    const data = await r.json();
    const jobs = data.results || [];
    if (!jobs.length) {
      area.innerHTML = '<div class="empty">No pipelines submitted yet.</div>';
      return;
    }
    const rows = jobs.map(j => `
      <tr>
        <td><code>#${j.id}</code></td>
        <td>${shortPath(j.dataset_path)}</td>
        <td>ws=${j.window_size}, r=${j.num_reducers}</td>
        <td><span class="status status-${j.status}">${j.status}</span></td>
        <td>${renderProgress(j.live)}</td>
        <td class="mono">${j.yarn_app_id || '—'}</td>
        <td>${fmtDuration(j.duration_s)}</td>
        <td class="mono">${j.output_hdfs_path ? shortPath(j.output_hdfs_path) : '—'}</td>
      </tr>
    `).join('');
    area.innerHTML = `
      <table>
        <thead>
          <tr>
            <th>ID</th><th>Corpus</th><th>Params</th><th>Status</th>
            <th>Progress</th><th>YARN app</th><th>Duration</th><th>Output</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>`;
  } catch (err) {
    area.innerHTML = `<div class="empty">Network error: ${err.message}</div>`;
  }
}

refreshJobs();
setInterval(refreshJobs, 5000);
