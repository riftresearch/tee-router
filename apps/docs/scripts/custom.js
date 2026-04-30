(function() {
  function removePoweredBy() {
    const links = document.querySelectorAll('a[href*="mintlify.com"]');
    links.forEach(link => {
      if (link.textContent.includes('Powered by'))  {
        link.remove();
      }
    });
  }

  // Run on load
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', removePoweredBy);
  } else {
    removePoweredBy();
  }

  // Run on navigation (for SPA behavior)
  const observer = new MutationObserver(removePoweredBy);
  observer.observe(document.body, { childList: true, subtree: true });
})();
