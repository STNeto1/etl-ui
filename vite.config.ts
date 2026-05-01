import { defineConfig } from "vitest/config";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import babel from "@rolldown/plugin-babel";
import tailwindcss from "@tailwindcss/vite";

// https://vite.dev/config/
export default defineConfig({
  assetsInclude: ["**/*.wasm"],
  worker: {
    format: "es",
  },
  plugins: [tailwindcss(), react(), babel({ presets: [reactCompilerPreset()] })],
  test: {
    environment: "jsdom",
    setupFiles: ["./vitest.setup.ts"],
    testTimeout: 10000,
    env: {
      VITEST: "true",
    },
  },
});
