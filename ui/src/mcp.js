import './styles.css';

// Keep examples copyable without ever handling a real API key in the page.
document.querySelectorAll('.copy-code').forEach((button) => {
  button.addEventListener('click', async () => {
    const code = button.closest('.code')?.querySelector('pre')?.textContent;
    if (!code) return;
    try {
      await navigator.clipboard.writeText(code);
      const original = button.textContent;
      button.textContent = 'Copied';
      setTimeout(() => { button.textContent = original; }, 1600);
    } catch {
      button.textContent = 'Copy unavailable';
    }
  });
});
