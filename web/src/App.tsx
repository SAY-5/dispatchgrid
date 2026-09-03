import { Footer } from "./components/Footer";
import { Nav } from "./components/Nav";
import { GeoMatch } from "./sections/GeoMatch";
import { Hero } from "./sections/Hero";
import { LoadRun } from "./sections/LoadRun";
import { Rollout } from "./sections/Rollout";
import { Streams } from "./sections/Streams";
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
        <LoadRun />
      </main>
      <Footer />
    </>
  );
}
