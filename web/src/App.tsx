import { Nav } from "./components/Nav";
import { Hero } from "./sections/Hero";
import { Streams } from "./sections/Streams";
import { GeoMatch } from "./sections/GeoMatch";
import { Rollout } from "./sections/Rollout";
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
        <Streams />
        <GeoMatch />
        <Rollout />
      </main>
    </>
  );
}
