import { Nav } from "./components/Nav";
import { Hero } from "./sections/Hero";
import "./styles/sections.css";

export function App() {
  return (
    <>
      <a className="sr-only" href="#top">
        Skip to content
      </a>
      <Nav />
      <main>
        <Hero />
      </main>
    </>
  );
}
