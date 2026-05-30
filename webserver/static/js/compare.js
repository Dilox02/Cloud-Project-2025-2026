const COLORS = [
  '#2563eb', '#dc2626', '#16a34a', '#d97706',
  '#7c3aed', '#0891b2', '#db2777', '#65a30d',
];

let allJobs = [];
let charts  = {};

function jobLabel(j) {
  const ds = j.dataset_path.split('/').filter(Boolean).pop() || j.dataset_path;
  return `#${j.id} ${j.engine} — ${ds} ws=${j.window_size} r=${j.num_reducers}`;
}

function shortLabel(j) {
  return `#${j.id} ${j.engine}`;
}

function buildChart(id, label, values, labels, colors) {
  if (charts[id]) charts[id].destroy();
  const ctx = document.getElementById(id).getContext('2d');
  charts[id] = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label,
        data: values,
        backgroundColor: colors.map(c => c + 'cc'),
        borderColor: colors,
        borderWidth: 2,
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      plugins: { legend: { display: false } },
      scales: {
        y: { beginAtZero: true, grid: { color: '#e5e7eb' } },
        x: { grid: { display: false } },
      },
    },
  });
}

function updateCharts() {
  const selected = allJobs.filter(j =>
    document.getElementById(`cb-${j.id}`)?.checked
  );

  const noSel    = document.getElementById('no-selection');
  const chartsEl = document.getElementById('charts-area');

  if (!selected.length) {
    noSel.style.display    = '';
    chartsEl.style.display = 'none';
    return;
  }
  noSel.style.display    = 'none';
  chartsEl.style.display = '';

  const labels    = selected.map(shortLabel);
  const colors    = selected.map((_, i) => COLORS[i % COLORS.length]);
  const times     = selected.map(j => j.duration_s ?? null);
  const mems      = selected.map(j => j.peak_memory_mb ?? null);
  const thrput    = selected.map(j =>
    (j.dataset_size_mb && j.execution_time_s)
      ? +(j.dataset_size_mb / j.execution_time_s).toFixed(2)
      : null
  );

  buildChart('chart-time',       'Total duration (s)',        times,  labels, colors);
  buildChart('chart-mem',        'Avg allocated memory (MB)', mems,   labels, colors);
  buildChart('chart-throughput', 'Throughput (MB/s)',          thrput, labels, colors);
}

function renderSidebar(jobs) {
  const list = document.getElementById('job-list');
  if (!jobs.length) {
    list.innerHTML = '<div class="empty">No completed jobs found.</div>';
    return;
  }

  list.innerHTML = jobs.map((j, i) => `
    <label class="job-check-row" style="--color:${COLORS[i % COLORS.length]}">
      <input type="checkbox" id="cb-${j.id}" value="${j.id}">
      <span class="job-check-dot"></span>
      <span class="job-check-label">${jobLabel(j)}</span>
    </label>
  `).join('');

  list.querySelectorAll('input[type=checkbox]').forEach(cb =>
    cb.addEventListener('change', updateCharts)
  );

  const btn = document.getElementById('select-all-btn');
  btn.style.display = '';
  btn.addEventListener('click', () => {
    const boxes = list.querySelectorAll('input[type=checkbox]');
    const allChecked = [...boxes].every(b => b.checked);
    boxes.forEach(b => { b.checked = !allChecked; });
    updateCharts();
  });
}

async function init() {
  try {
    const r    = await fetch('/api/jobs/compare/');
    allJobs    = await r.json();
    renderSidebar(allJobs);
  } catch (err) {
    document.getElementById('job-list').innerHTML =
      `<div class="empty">Error: ${err.message}</div>`;
  }
}

init();
