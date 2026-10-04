/* Display the existing lot messages in the site, with the same acknowledgement flow. */
(() => {
  'use strict';
  const dialog = document.getElementById('lot-feedback-dialog');
  const message = document.getElementById('lot-feedback-message');
  const create = document.getElementById('create-lots-btn');
  const confirm = document.getElementById('confirm-lot-creation-btn');
  let busy = false, acknowledge, returnFocus;

  function show(text) {
    // Keep the original wording; omit the legacy status emoji in this presentation.
    const displayText = text.replace(/^(?:✅|❌|⚠\uFE0F?)[ \t]*/u, '');
    let outcome = text.startsWith('❌') ? 'error' : text.startsWith('⚠') ? 'warning' : text.startsWith('✅') ? 'success' : 'info';
    const counts = displayText.match(/(?:^|\n)Success: (\d+) \| Failed: (\d+)(?:\n|$)/);
    if (counts) {
      const succeeded = Number(counts[1]), failed = Number(counts[2]);
      outcome = failed ? (succeeded ? 'warning' : 'error') : (succeeded ? 'success' : 'info');
    }
    dialog.dataset.outcome = outcome;
    message.replaceChildren();
    displayText.split(/(\n{2,})/).forEach((block, index) => {
      if (index % 2) {
        message.append(document.createTextNode(block));
        return;
      }
      if (!block) return;
      const paragraph = document.createElement('p');
      paragraph.className = 'site-feedback-paragraph';
      if (index === 0) paragraph.classList.add('site-feedback-lead');
      if (block.startsWith('Success: ')) paragraph.classList.add('site-feedback-totals');
      if (block.startsWith('- Vendor Lot ')) paragraph.classList.add('site-feedback-details');
      paragraph.textContent = block;
      message.append(paragraph);
    });
    returnFocus = document.activeElement;
    return new Promise(resolve => {
      acknowledge = resolve;
      dialog.showModal();
    });
  }

  function begin() {
    busy = true;
    [create, confirm].forEach(button => {
      if (button) {
        button.disabled = true;
        button.setAttribute('aria-busy', 'true');
      }
    });
  }

  function end() {
    busy = false;
    [create, confirm].forEach(button => {
      if (button) {
        button.disabled = false;
        button.removeAttribute('aria-busy');
      }
    });
    create?.focus({preventScroll: true});
  }

  document.getElementById('lot-feedback-close').addEventListener('click', () => dialog.close());
  dialog.addEventListener('close', () => {
    if (returnFocus?.isConnected && !returnFocus.disabled) returnFocus.focus({preventScroll: true});
    const resolve = acknowledge;
    acknowledge = null;
    resolve?.();
  });
  window.StokesLots = {get busy() { return busy; }, show, begin, end};
})();
