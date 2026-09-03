const LINKS = [
  { href: "#streams", label: "Streams" },
  { href: "#geo", label: "Geo match" },
  { href: "#rollout", label: "Rollout" },
  { href: "#load", label: "Load run" },
];

export function Nav() {
  return (
    <header className="nav">
      <div className="wrap nav-inner">
        <a href="#top" className="brand" aria-label="DispatchGrid home">
          <span className="brand-mark" aria-hidden="true" />
          <span>DispatchGrid</span>
        </a>
        <nav aria-label="Sections">
          <ul>
            {LINKS.map((l) => (
              <li key={l.href}>
                <a href={l.href}>{l.label}</a>
              </li>
            ))}
            <li>
              <a href="https://github.com/SAY-5/dispatchgrid" target="_blank" rel="noreferrer">
                Repo
              </a>
            </li>
          </ul>
        </nav>
      </div>
    </header>
  );
}
