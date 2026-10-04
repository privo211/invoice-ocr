/* Real PDF selection and multipart submission. Icons: Lucide, ISC (icons-LICENSE.txt). */
(() => {
  'use strict';
  const form = document.getElementById('extract-form');
  if (!form) return;
  const dropzone = document.getElementById('dropzone');
  const pdfInput = document.getElementById('pdfs');
  const fileInfo = document.getElementById('file-info');
  const browse = document.getElementById('browse-files');
  const clear = document.getElementById('clear-files');
  const transfer = new DataTransfer();
  const fileIcon = '<svg class="site-file-symbol" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6M8 13h8m-8 4h8"/></svg>';

  function updateFiles() {
    const files = Array.from(transfer.files);
    pdfInput.files = transfer.files;
    fileInfo.replaceChildren();
    document.getElementById('file-count').textContent = `${files.length} ${files.length === 1 ? 'file' : 'files'} selected`;
    clear.disabled = files.length === 0;
    if (!files.length) {
      const empty = document.createElement('p');
      empty.className = 'site-file-empty'; empty.textContent = 'No files chosen (max. 25 MB)';
      fileInfo.append(empty); return;
    }
    const list = document.createElement('ul');
    list.className = 'site-file-list'; list.setAttribute('aria-label', 'Selected PDF files');
    files.forEach((file, index) => {
      const row = document.createElement('li'); row.className = 'site-file-row';
      row.innerHTML = fileIcon; // Constant SVG only; filenames use text nodes.
      const details = document.createElement('div'); details.className = 'site-file-details';
      const name = document.createElement('div'); name.className = 'site-file-name';
      file.name.split(/(?<=[_-])/).forEach(part => { name.append(document.createTextNode(part), document.createElement('wbr')); });
      const size = document.createElement('div'); size.className = 'site-file-size';
      size.textContent = file.size < 1024 * 1024 ? `${Math.max(1, Math.round(file.size / 1024))} KB` : `${(file.size / (1024 * 1024)).toFixed(1)} MB`;
      details.append(name, size);
      const remove = document.createElement('button');
      remove.type = 'button'; remove.className = 'site-remove-file'; remove.textContent = 'Remove';
      remove.setAttribute('aria-label', `Remove ${file.name}`);
      remove.dataset.siteTooltip = 'Remove this file from the current upload.';
      remove.addEventListener('click', () => {
        const remaining = Array.from(transfer.files).filter((_, i) => i !== index);
        transfer.items.clear(); remaining.forEach(file => transfer.items.add(file));
        updateFiles();
        const next = fileInfo.querySelectorAll('.site-remove-file')[Math.min(index, remaining.length - 1)];
        (next || browse).focus();
      });
      row.append(details, remove); list.append(row);
    });
    fileInfo.append(list);
  }
  function addFiles(files) {
    // Preserve the existing PDF MIME check, selection order, and name/size deduplication.
    Array.from(files).forEach(file => {
      if (file.type === 'application/pdf' && !Array.from(transfer.files).some(existing => existing.name === file.name && existing.size === file.size)) transfer.items.add(file);
    });
    updateFiles();
  }
  browse.addEventListener('click', () => pdfInput.click());
  dropzone.addEventListener('click', event => {
    if (!event.target.closest('button, input')) pdfInput.click();
  });
  pdfInput.addEventListener('change', () => addFiles(pdfInput.files));
  clear.addEventListener('click', () => { transfer.items.clear(); pdfInput.value = ''; updateFiles(); browse.focus(); });
  ['dragenter', 'dragover'].forEach(event => dropzone.addEventListener(event, e => { e.preventDefault(); dropzone.classList.add('dragover'); }));
  dropzone.addEventListener('dragleave', e => { if (!dropzone.contains(e.relatedTarget)) dropzone.classList.remove('dragover'); });
  dropzone.addEventListener('drop', e => { e.preventDefault(); dropzone.classList.remove('dragover'); addFiles(e.dataTransfer.files); });
  form.addEventListener('submit', () => {
    document.getElementById('loading-overlay').hidden = false;
    form.setAttribute('aria-busy', 'true');
  });
  window.addEventListener('pageshow', event => {
    if (event.persisted) { document.getElementById('loading-overlay').hidden = true; form.removeAttribute('aria-busy'); }
  });
})();
