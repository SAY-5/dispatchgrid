# Contributing

* Java 21, Maven, Docker. `make lint test` must pass before a pull request; CI runs the same
  targets plus `scripts/k8s-e2e.sh` on kind.
* Formatting is google-java-format via Spotless: `make fmt`.
* Unit tests live next to the code as `*Test.java` and run with Surefire; anything that needs
  Docker is an `*IT.java` run by Failsafe with Testcontainers.
* Keep changes to the wire format (`common/model`, `common/serde`) backward compatible; the
  topics are consumed by more than one service.
* Commit messages follow conventional commits on a single line, for example
  `fix(matching): release claim when trip is no longer REQUESTED`.
