import { defineConfig } from "tsup";

export default defineConfig((options) => {
  return [
    {
      entry: {
        index: "src/node/index.ts",
      },
      watch: options.watch,
      target: "es2022",
      platform: "node",
      format: ["esm"],
      splitting: true,
      shims: false,
      minify: false,
      sourcemap: true,
      dts: true,
      clean: true,
      env: {
        ...options.env,
      },
    },
    {
      entry: {
        react: "src/react/index.ts",
      },
      watch: options.watch,
      target: "es2022",
      platform: "neutral",
      format: ["esm"],
      splitting: true,
      shims: false,
      minify: false,
      sourcemap: true,
      dts: true,
      clean: true,
      env: {
        ...options.env,
      },
    },
    {
      entry: {
        core: "src/common/index.ts",
      },
      watch: options.watch,
      target: "es2022",
      platform: "neutral",
      format: ["esm"],
      splitting: true,
      shims: false,
      minify: false,
      sourcemap: true,
      dts: true,
      clean: true,
      env: {
        ...options.env,
      },
    },
  ];
});
