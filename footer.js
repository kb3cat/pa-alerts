(function () {
  const footer = document.createElement("footer");

  footer.innerHTML = `
    <div class="site-footer-inner">
      <a href="https://kb3cat.com/contact.html">Contact / Suggestions</a>
    </div>
  `;

  footer.className = "site-footer";

  const style = document.createElement("style");
  style.textContent = `
    .site-footer {
      width: 100%;
      margin-top: 32px;
      padding: 14px 0;
      border-top: 1px solid rgba(255,255,255,0.08);
      text-align: center;
      font-size: 12px;
      color: #888;
    }

    .site-footer a {
      color: #888;
      text-decoration: none;
    }

    .site-footer a:hover {
      color: #fff;
      text-decoration: underline;
    }
  `;

  document.head.appendChild(style);
  document.body.appendChild(footer);
})();
