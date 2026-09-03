export function Footer() {
  return (
    <footer className="footer">
      <div className="wrap footer-inner">
        <div>
          <p className="footer-title">DispatchGrid</p>
          <p className="dim">
            This page is a browser port of the real services. The production stack is Java 21 and Spring Boot 3 with Kafka
            Streams, MySQL 8 shards, Redis 7 GEO, and Kubernetes rolling updates; the matcher, shard router, geo index,
            claim script, stats window, and load generator here are the same logic in TypeScript, driven by a seeded
            simulated clock. Headline numbers are from a measured run of the real stack, service time in the browser is
            synthetic.
          </p>
        </div>
        <ul className="footer-links">
          <li>
            <a href="https://github.com/SAY-5/dispatchgrid" target="_blank" rel="noreferrer">
              github.com/SAY-5/dispatchgrid
            </a>
          </li>
          <li>
            <a href="https://github.com/SAY-5/dispatchgrid/blob/main/ARCHITECTURE.md" target="_blank" rel="noreferrer">
              Architecture notes
            </a>
          </li>
          <li>
            <a href="https://github.com/SAY-5/dispatchgrid/tree/main/web" target="_blank" rel="noreferrer">
              Source of this page
            </a>
          </li>
        </ul>
      </div>
    </footer>
  );
}
