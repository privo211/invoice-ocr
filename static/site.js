/* Shared appearance only. No polling, uploads, or external requests. */
(() => {
  'use strict';
  const root = document.documentElement;
  const themeKey = 'stokes-ui-theme';
  let theme = 'light';
  try {
    // Accept both historic values used by home/results and logs.
    const saved = localStorage.getItem(themeKey) ?? localStorage.getItem('theme');
    theme = saved === 'dark' || saved === 'dark-mode' ? 'dark' : 'light';
  } catch (_) { /* Theme controls remain usable without local storage. */ }
  function applyTheme(value) {
    theme = value;
    const dark = theme === 'dark';
    root.dataset.theme = theme;
    root.dataset.bsTheme = theme;
    if (!document.body) return;
    document.body.classList.toggle('dark-mode', dark);
    const button = document.getElementById('theme-toggle');
    if (button) {
      button.setAttribute('aria-label', `Switch to ${dark ? 'light' : 'dark'} mode`);
      button.dataset.siteTooltip = `Use ${dark ? 'light' : 'dark'} appearance.`;
      const label = button.querySelector('.site-theme-text');
      if (label) label.textContent = dark ? 'Light Mode' : 'Dark Mode';
    }
    document.querySelectorAll('.btn-close').forEach(button => button.classList.toggle('btn-close-white', dark));
  }
  // This runs in the head, before the first paint, to prevent a light-theme flash.
  applyTheme(theme);
  function initializeTooltips() {
    // Keep screen-reader names for switches without showing redundant hints.
    const switchLabels = [
      ['GPCertificationOutstanding', 'G&P Certification Outstanding'],
      ['UnderWeightExemption', 'Underweight Exemption'],
      ['GermSampleRequired', 'Germ Sample Required'],
    ];
    switchLabels.forEach(([field, label]) => {
      document.querySelectorAll(`[data-field="${field}"]`).forEach(element => {
        if (!element.hasAttribute('aria-label')) element.setAttribute('aria-label', label);
      });
    });
    // Describe only controls where a short hint adds context. Business handlers stay unchanged.
    const hints = [
      ['#browse-files', 'Add one or more PDFs. Files already selected are kept.'],
      ['#clear-files', 'Remove all selected files from this upload.'],
      ['.extract-data', 'Read the documents and open the results for review.'],
      ['.site-nav a[href$="/logs"]', 'View document processing history and extraction statistics.'],
      ['.lookup-btn[data-bs-target="#lookup-modal-1"]', 'Choose one or more treatments from the list.', 'Choose treatments'],
      ['.lookup-btn[data-bs-target="#lookup-modal-2"]', 'Choose one or more treatment coatings from the list.', 'Choose treatment coatings'],
      ['.po-field', 'Enter the purchase order, then leave this field to load its items.', 'Search Purchase Order'],
      ['select[data-field="BCItemNo"]', 'Choose the Business Central item for this lot. Other opens the full item list.', 'Business Central item'],
      ['select[id^="bc-input-"]', 'Choose from the full list of Business Central items.', 'Business Central item'],
      ['[data-field="CustomerPO"]', 'Use the five-digit customer purchase order number.', 'Customer PO'],
      ['[data-field="InboundTrackingNo"]', 'Enter the carrier tracking number, if available.', 'Inbound Tracking #'],
      ['#create-lots-btn', 'Review and select the lots to create in Business Central.'],
      ['.create-invoice-btn', 'Create this invoice in Business Central using the values shown.'],
      ['#update-selected-btn', 'Select the shipping reports to update in Business Central.'],
      ['.no-input', 'Enter the Business Central item or resource number for this line.', 'Business Central item or resource number'],
      ['.qty-input', 'Changes update the line subtotal and invoice total.', 'Quantity'],
      ['.cost-input', 'Cost per unit. Changes update the subtotal and invoice total.', 'Unit cost'],
    ];
    hints.forEach(([selector, text, label]) => {
      document.querySelectorAll(selector).forEach(element => {
        element.dataset.siteTooltip = text;
        if (label && !element.hasAttribute('aria-label')) element.setAttribute('aria-label', label);
      });
    });

    const tip = document.createElement('div');
    tip.id = 'site-tooltip'; tip.className = 'site-tooltip'; tip.setAttribute('role', 'tooltip'); tip.hidden = true;
    document.body.append(tip);
    let active = null, hovered = null, focused = null, dismissed = null, timer, leaveTimer;
    const triggerFor = target => target instanceof Element ? target.closest('[data-site-tooltip]') : null;
    function hide() {
      clearTimeout(timer);
      clearTimeout(leaveTimer);
      if (active) {
        const ids = (active.getAttribute('aria-describedby') || '').split(/\s+/).filter(id => id && id !== tip.id);
        if (ids.length) active.setAttribute('aria-describedby', ids.join(' '));
        else active.removeAttribute('aria-describedby');
      }
      tip.hidden = true; active = null;
    }
    function show(trigger, delay = 350) {
      if (!trigger || trigger.disabled || trigger === dismissed || active === trigger) return;
      hide(); active = trigger;
      timer = setTimeout(() => {
        const rect = trigger.getBoundingClientRect();
        if (!trigger.isConnected || !rect.width || rect.bottom <= 0 || rect.top >= innerHeight) { hide(); return; }
        tip.textContent = trigger.dataset.siteTooltip;
        tip.hidden = false;
        const width = tip.offsetWidth, height = tip.offsetHeight;
        const left = Math.max(12, Math.min(rect.left + (rect.width - width) / 2, root.clientWidth - width - 12));
        const top = rect.top - height - 8 >= 12 ? rect.top - height - 8 : Math.min(rect.bottom + 8, innerHeight - height - 12);
        tip.style.left = `${left}px`; tip.style.top = `${top}px`;
        const ids = (trigger.getAttribute('aria-describedby') || '').split(/\s+/).filter(Boolean);
        if (!ids.includes(tip.id)) trigger.setAttribute('aria-describedby', [...ids, tip.id].join(' '));
      }, delay);
    }
    function dismiss() { dismissed = active || focused || hovered; hide(); }
    document.addEventListener('pointerover', event => {
      if (event.pointerType === 'touch') return;
      if (tip.contains(event.target)) { clearTimeout(leaveTimer); hovered = active; return; }
      const trigger = triggerFor(event.target);
      if (!trigger || trigger.contains(event.relatedTarget)) return;
      clearTimeout(leaveTimer); dismissed = null; hovered = trigger; show(trigger);
    });
    document.addEventListener('pointerout', event => {
      const trigger = triggerFor(event.target);
      if (trigger && (trigger.contains(event.relatedTarget) || tip.contains(event.relatedTarget))) return;
      if (tip.contains(event.target) && active?.contains(event.relatedTarget)) return;
      if (trigger || tip.contains(event.target)) {
        hovered = null;
        // Leave a short bridge so the pointer can move onto the tooltip itself.
        leaveTimer = setTimeout(() => { if (focused) show(focused, 0); else hide(); }, 120);
      }
    });
    document.addEventListener('focusin', event => { dismissed = null; focused = triggerFor(event.target); if (focused) show(focused, 0); });
    document.addEventListener('focusout', () => { focused = null; if (hovered) show(hovered); else hide(); });
    document.addEventListener('keydown', event => { if (event.key === 'Escape') dismiss(); });
    // Dismiss while editing or opening a control; never intercept its normal action.
    ['pointerdown', 'click', 'input', 'change'].forEach(event => document.addEventListener(event, dismiss));
    document.addEventListener('scroll', () => {
      hide();
      // Keyboard focus can scroll a field into view. Show its hint after scrolling settles.
      if (focused?.matches(':focus-visible')) show(focused, 120);
    }, true);
    window.addEventListener('resize', dismiss);
  }
  function initialize() {
    applyTheme(theme);
    initializeTooltips();
    document.getElementById('theme-toggle')?.addEventListener('click', () => {
      applyTheme(theme === 'dark' ? 'light' : 'dark');
      // Keep the previous UI's preference untouched for a rollback.
      try { localStorage.setItem(themeKey, theme); } catch (_) { /* Optional persistence. */ }
    });
    const reduced = matchMedia('(prefers-reduced-motion: reduce)');
    if (!reduced.matches) {
      document.querySelectorAll('[data-site-enter]').forEach(element => {
        element.classList.add('site-enter');
        element.addEventListener('animationend', () => element.classList.remove('site-enter'), { once: true });
      });
    }
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', initialize);
  else initialize();
  // Keep already-open site tabs in sync with the canonical theme preference.
  window.addEventListener('storage', event => {
    if (event.key === themeKey) applyTheme(event.newValue === 'dark' || event.newValue === 'dark-mode' ? 'dark' : 'light');
  });
})();
