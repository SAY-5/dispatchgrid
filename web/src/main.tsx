import React from "react";
import { createRoot } from "react-dom/client";
import { App } from "./App";
import { formatSelfCheck, runSelfCheck } from "./sim/selfcheck";
import "./styles/theme.css";

declare global {
  interface Window {
    dispatchgridSelfCheck: () => string;
  }
}

/** Run the sim self check from the console: dispatchgridSelfCheck() */
window.dispatchgridSelfCheck = () => {
  const text = formatSelfCheck(runSelfCheck());
  console.log(text);
  return text;
};

const el = document.getElementById("root");
if (el) {
  createRoot(el).render(
    <React.StrictMode>
      <App />
    </React.StrictMode>,
  );
}
