/* ==========================================================================
   ECS Data Pipeline — Client Application
   Handles tabs, file upload, DB explorer, SQL editor, and history
   ========================================================================== */

(() => {
    'use strict';

    const API = '';

    // ── DOM refs ──────────────────────────────────────────────────────────
    const $ = (sel, ctx = document) => ctx.querySelector(sel);
    const $$ = (sel, ctx = document) => [...ctx.querySelectorAll(sel)];

    // Header
    const dbStatus = $('#db-status');
    const statusDot = $('.status-dot');
    const statusText = $('.status-text');

    // Tabs
    const tabs = $$('.tab');
    const panels = $$('.panel');

    // Upload
    const dropZone = $('#drop-zone');
    const fileInput = $('#file-input');
    const folderInput = $('#folder-input');
    const btnBrowseFiles = $('#btn-browse-files');
    const btnBrowseFolder = $('#btn-browse-folder');
    const uploadResults = $('#upload-results');
    const uploadResultsList = $('#upload-results-list');

    // Explorer
    const tableList = $('#table-list');
    const tableDetail = $('#table-detail');

    // SQL
    const btnExecute = $('#btn-execute');
    const readonlyToggle = $('#readonly-toggle');
    const historyList = $('#history-list');
    const resultsHeader = $('#results-header');
    const resultsInfo = $('#results-info');
    const resultsPlaceholder = $('#results-placeholder');
    const resultsTableWrap = $('#results-table-wrap');
    const resultsThead = $('#results-thead');
    const resultsTbody = $('#results-tbody');
    const btnExportCsv = $('#btn-export-csv');

    let editor = null; // CodeMirror instance
    let lastQueryRows = [];
    let lastQueryCols = [];

    // ── TOAST ─────────────────────────────────────────────────────────────
    function toast(msg, type = 'info', duration = 4000) {
        const container = $('#toast-container');
        const el = document.createElement('div');
        el.className = `toast ${type}`;
        el.textContent = msg;
        container.appendChild(el);
        setTimeout(() => {
            el.classList.add('fade-out');
            el.addEventListener('animationend', () => el.remove());
        }, duration);
    }

    // ── TABS ──────────────────────────────────────────────────────────────
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            tabs.forEach(t => t.classList.remove('active'));
            panels.forEach(p => p.classList.remove('active'));
            tab.classList.add('active');
            $(`#panel-${tab.dataset.tab}`).classList.add('active');

            if (tab.dataset.tab === 'explorer') loadTables();
            if (tab.dataset.tab === 'sql') initEditor();
        });
    });

    // ── DB STATUS ─────────────────────────────────────────────────────────
    async function checkDbStatus() {
        try {
            const res = await fetch(`${API}/api/tables`);
            if (res.ok) {
                statusDot.classList.add('connected');
                statusDot.style.animation = 'none';
                const data = await res.json();
                const total = data.tables.reduce((s, t) => s + t.row_count, 0);
                statusText.textContent = `Connected · ${formatNum(total)} rows`;
            }
        } catch {
            statusText.textContent = 'Disconnected';
        }
    }

    // ── UPLOAD ────────────────────────────────────────────────────────────
    // Drag & drop
    dropZone.addEventListener('dragover', e => { e.preventDefault(); dropZone.classList.add('drag-over'); });
    dropZone.addEventListener('dragleave', () => dropZone.classList.remove('drag-over'));
    dropZone.addEventListener('drop', e => {
        e.preventDefault();
        dropZone.classList.remove('drag-over');
        const items = e.dataTransfer.items;
        const files = [];

        // Collect files (including from directory entries)
        const promises = [];
        for (const item of items) {
            const entry = item.webkitGetAsEntry?.();
            if (entry) {
                promises.push(readEntry(entry, files));
            } else if (item.kind === 'file') {
                files.push(item.getAsFile());
            }
        }
        Promise.all(promises).then(() => {
            if (files.length) uploadFiles(files);
        });
    });

    function readEntry(entry, files) {
        return new Promise(resolve => {
            if (entry.isFile) {
                entry.file(f => { files.push(f); resolve(); });
            } else if (entry.isDirectory) {
                const reader = entry.createReader();
                reader.readEntries(entries => {
                    Promise.all(entries.map(e => readEntry(e, files))).then(resolve);
                });
            } else {
                resolve();
            }
        });
    }

    // Click to browse
    btnBrowseFiles.addEventListener('click', e => { e.stopPropagation(); fileInput.click(); });
    btnBrowseFolder.addEventListener('click', e => { e.stopPropagation(); folderInput.click(); });
    dropZone.addEventListener('click', () => fileInput.click());

    fileInput.addEventListener('change', () => {
        if (fileInput.files.length) uploadFiles([...fileInput.files]);
        fileInput.value = '';
    });
    folderInput.addEventListener('change', () => {
        if (folderInput.files.length) uploadFiles([...folderInput.files]);
        folderInput.value = '';
    });

    async function uploadFiles(files) {
        // Only CSV files
        const csvFiles = files.filter(f => f.name.toLowerCase().endsWith('.csv'));
        if (!csvFiles.length) { toast('No CSV files found', 'warn'); return; }

        uploadResults.style.display = 'block';
        uploadResultsList.innerHTML = `
            <div class="upload-spinner">
                <div class="spinner"></div>
                Processing ${csvFiles.length} file(s)…
            </div>`;

        const formData = new FormData();
        csvFiles.forEach(f => formData.append('files', f));

        try {
            const res = await fetch(`${API}/api/upload`, { method: 'POST', body: formData });
            const data = await res.json();

            if (!res.ok) {
                toast(data.detail || 'Upload failed', 'error');
                uploadResultsList.innerHTML = '';
                return;
            }

            renderUploadResults(data.results);
            checkDbStatus(); // refresh counts
            const successCount = data.results.filter(r => r.status === 'success').length;
            if (successCount) toast(`${successCount} file(s) processed successfully`, 'success');
        } catch (err) {
            toast(`Upload error: ${err.message}`, 'error');
            uploadResultsList.innerHTML = '';
        }
    }

    function renderUploadResults(results) {
        uploadResultsList.innerHTML = results.map((r, i) => {
            const iconClass = r.status === 'success' ? 'success' : r.status === 'skipped' ? 'skipped' : 'error';
            const icon = r.status === 'success' ? '✓' : r.status === 'skipped' ? '⟳' : '✕';
            const typeClass = (r.type || '').toLowerCase();
            const detail = r.status === 'success'
                ? `${formatNum(r.rows_inserted)} rows inserted · HH ${r.hhid} · Sensor ${r.sensor_id}`
                : r.reason || 'Unknown error';

            return `
                <div class="result-card" style="animation-delay: ${i * 60}ms">
                    <div class="result-icon ${iconClass}">${icon}</div>
                    <div class="result-info">
                        <div class="result-file">${escHtml(r.file)}</div>
                        <div class="result-detail">${escHtml(detail)}</div>
                    </div>
                    ${r.type ? `<span class="result-badge ${typeClass}">${r.type}</span>` : ''}
                </div>`;
        }).join('');
    }

    // ── EXPLORER ──────────────────────────────────────────────────────────
    let tablesData = [];

    async function loadTables() {
        try {
            const res = await fetch(`${API}/api/tables`);
            const data = await res.json();
            tablesData = data.tables;
            renderTableList(tablesData);
        } catch {
            tableList.innerHTML = '<p class="empty-state">Failed to load tables</p>';
        }
    }

    function renderTableList(tables) {
        if (!tables.length) { tableList.innerHTML = '<p class="empty-state">No tables found</p>'; return; }
        tableList.innerHTML = tables.map(t => `
            <div class="table-item" data-table="${escHtml(t.name)}">
                <span>${escHtml(t.name)}</span>
                <span class="row-count">${formatNum(t.row_count)}</span>
            </div>
        `).join('');

        $$('.table-item', tableList).forEach(item => {
            item.addEventListener('click', () => {
                $$('.table-item', tableList).forEach(i => i.classList.remove('active'));
                item.classList.add('active');
                loadTableDetail(item.dataset.table);
            });
        });
    }

    async function loadTableDetail(tableName) {
        tableDetail.innerHTML = '<div class="loading-shimmer"></div>';
        try {
            const res = await fetch(`${API}/api/schema/${encodeURIComponent(tableName)}`);
            const data = await res.json();
            renderTableDetail(data);
        } catch {
            tableDetail.innerHTML = '<div class="detail-placeholder"><p>Failed to load table details</p></div>';
        }
    }

    function renderTableDetail(data) {
        const colsHtml = `
            <div class="schema-section">
                <h4>Columns <span class="row-count-badge">${data.columns.length} columns · ${formatNum(data.row_count)} rows</span></h4>
                <div class="data-table-wrap">
                    <table class="data-table">
                        <thead><tr><th>#</th><th>Name</th><th>Type</th><th>Not Null</th></tr></thead>
                        <tbody>
                            ${data.columns.map(c => `
                                <tr>
                                    <td>${c.cid}</td>
                                    <td style="font-weight:600">${escHtml(c.name)}</td>
                                    <td><span class="col-type">${escHtml(c.type || 'ANY')}</span></td>
                                    <td>${c.notnull ? '✓' : '—'}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>`;

        let sampleHtml = '';
        if (data.sample.length) {
            const cols = Object.keys(data.sample[0]);
            sampleHtml = `
                <div class="schema-section">
                    <h4>Sample Data (first ${data.sample.length} rows)</h4>
                    <div class="data-table-wrap" style="max-height:360px; overflow-y:auto;">
                        <table class="data-table" id="explorer-sample-table">
                            <thead><tr>${cols.map(c => `<th data-col="${escHtml(c)}">${escHtml(c)} <span class="sort-arrow">↕</span></th>`).join('')}</tr></thead>
                            <tbody>
                                ${data.sample.map(row => `<tr>${cols.map(c => `<td>${escHtml(String(row[c] ?? ''))}</td>`).join('')}</tr>`).join('')}
                            </tbody>
                        </table>
                    </div>
                </div>`;
        }

        tableDetail.innerHTML = colsHtml + sampleHtml;

        // Make explorer sample table sortable
        const sampleTable = $('#explorer-sample-table', tableDetail);
        if (sampleTable) makeSortable(sampleTable);
    }

    // ── SQL EDITOR ────────────────────────────────────────────────────────
    let editorInitialised = false;

    function initEditor() {
        if (editorInitialised) return;
        editorInitialised = true;

        editor = CodeMirror($('#sql-editor'), {
            mode: 'text/x-sql',
            theme: 'idea',
            lineNumbers: true,
            lineWrapping: true,
            indentWithTabs: false,
            smartIndent: true,
            matchBrackets: true,
            autofocus: true,
            extraKeys: {
                'Ctrl-Enter': executeQuery,
                'Cmd-Enter': executeQuery,
                'Ctrl-Space': 'autocomplete',
            },
            hintOptions: { tables: {} },
            value: 'SELECT * FROM fuel_meta LIMIT 20;',
        });

        // Load table names for autocomplete
        fetch(`${API}/api/tables`).then(r => r.json()).then(data => {
            const tableNames = {};
            data.tables.forEach(t => { tableNames[t.name] = []; });
            editor.setOption('hintOptions', { tables: tableNames });
        });

        loadHistory();
    }

    btnExecute.addEventListener('click', executeQuery);

    async function executeQuery() {
        const sql = editor.getValue().trim();
        if (!sql) { toast('Enter a SQL query first', 'warn'); return; }

        btnExecute.disabled = true;
        btnExecute.innerHTML = '<div class="spinner" style="width:16px;height:16px;border-width:2px"></div> Running…';

        try {
            const res = await fetch(`${API}/api/execute`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sql, read_only: readonlyToggle.checked }),
            });
            const data = await res.json();

            if (!res.ok) {
                toast(data.detail || 'Query failed', 'error');
                addHistoryEntry(sql, 0, 0, 'error', data.detail);
                return;
            }

            lastQueryCols = data.columns;
            lastQueryRows = data.rows;

            renderResults(data);
            addHistoryEntry(sql, data.row_count, data.duration_sec, 'success');
            if (data.truncated) toast(`Results truncated to ${formatNum(data.row_count)} rows`, 'warn');
        } catch (err) {
            toast(`Query error: ${err.message}`, 'error');
        } finally {
            btnExecute.disabled = false;
            btnExecute.innerHTML = `<svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor"><polygon points="5 3 19 12 5 21 5 3"/></svg> Execute`;
        }
    }

    function renderResults(data) {
        resultsPlaceholder.style.display = 'none';
        resultsHeader.style.display = 'flex';
        resultsTableWrap.style.display = 'block';
        btnExportCsv.style.display = 'inline-flex';

        resultsInfo.textContent = `${formatNum(data.row_count)} row${data.row_count !== 1 ? 's' : ''} · ${data.duration_sec}s`;

        if (!data.columns.length) {
            resultsThead.innerHTML = '';
            resultsTbody.innerHTML = '<tr><td style="text-align:center;padding:20px;color:var(--text-muted)">Query executed successfully (no result rows)</td></tr>';
            return;
        }

        resultsThead.innerHTML = `<tr>${data.columns.map(c => `<th data-col="${escHtml(c)}">${escHtml(c)} <span class="sort-arrow">↕</span></th>`).join('')}</tr>`;
        resultsTbody.innerHTML = data.rows.map(row =>
            `<tr>${data.columns.map(c => `<td>${escHtml(String(row[c] ?? ''))}</td>`).join('')}</tr>`
        ).join('');

        makeSortable($('#results-table'));
    }

    // ── Sortable tables ───────────────────────────────────────────────────
    function makeSortable(table) {
        const ths = $$('th', table);
        ths.forEach((th, colIdx) => {
            let asc = true;
            th.addEventListener('click', () => {
                const tbody = $('tbody', table);
                const rows = [...tbody.rows];
                rows.sort((a, b) => {
                    const aVal = a.cells[colIdx]?.textContent || '';
                    const bVal = b.cells[colIdx]?.textContent || '';
                    const aNum = parseFloat(aVal);
                    const bNum = parseFloat(bVal);
                    if (!isNaN(aNum) && !isNaN(bNum)) return asc ? aNum - bNum : bNum - aNum;
                    return asc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
                });
                rows.forEach(r => tbody.appendChild(r));

                // Update arrows
                $$('.sort-arrow', table).forEach(a => { a.classList.remove('active'); a.textContent = '↕'; });
                const arrow = $('.sort-arrow', th);
                if (arrow) { arrow.classList.add('active'); arrow.textContent = asc ? '↑' : '↓'; }

                asc = !asc;
            });
        });
    }

    // ── CSV EXPORT ────────────────────────────────────────────────────────
    btnExportCsv.addEventListener('click', () => {
        if (!lastQueryCols.length) return;
        const header = lastQueryCols.join(',');
        const body = lastQueryRows.map(row =>
            lastQueryCols.map(c => {
                const v = String(row[c] ?? '');
                return v.includes(',') || v.includes('"') || v.includes('\n')
                    ? `"${v.replace(/"/g, '""')}"`
                    : v;
            }).join(',')
        ).join('\n');
        const blob = new Blob([header + '\n' + body], { type: 'text/csv' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `query_results_${Date.now()}.csv`;
        a.click();
        URL.revokeObjectURL(url);
        toast('CSV exported', 'success', 2000);
    });

    // ── QUERY HISTORY ─────────────────────────────────────────────────────
    const HISTORY_KEY = 'ecs_query_history';

    function loadHistory() {
        try {
            const stored = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]');
            renderHistory(stored);
        } catch { /* ignore */ }
    }

    function addHistoryEntry(sql, rowCount, duration, status, error) {
        const entry = {
            sql, rowCount, duration, status, error,
            timestamp: new Date().toISOString(),
        };
        let history;
        try { history = JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); } catch { history = []; }
        history.unshift(entry);
        if (history.length > 50) history.length = 50;
        localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
        renderHistory(history);
    }

    function renderHistory(history) {
        if (!history.length) {
            historyList.innerHTML = '<p class="empty-state">No queries yet</p>';
            return;
        }
        historyList.innerHTML = history.map(h => {
            const time = new Date(h.timestamp);
            const timeStr = time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
            return `
                <div class="history-item ${h.status}" data-sql="${escAttr(h.sql)}">
                    <span class="h-sql">${escHtml(h.sql)}</span>
                    <div class="h-meta">
                        <span>${timeStr}</span>
                        <span>${h.rowCount ?? 0} rows</span>
                        <span>${h.duration ?? 0}s</span>
                    </div>
                </div>`;
        }).join('');

        $$('.history-item', historyList).forEach(item => {
            item.addEventListener('click', () => {
                if (editor) editor.setValue(item.dataset.sql);
            });
        });
    }

    // ── UTILITIES ─────────────────────────────────────────────────────────
    function escHtml(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    function escAttr(s) {
        return s.replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    }

    function formatNum(n) {
        return Number(n).toLocaleString('en-US');
    }

    // ── INIT ──────────────────────────────────────────────────────────────
    checkDbStatus();

})();
