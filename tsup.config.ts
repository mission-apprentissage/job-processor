import type { Options } from "tsup";
import { defineConfig } from "tsup";

export default defineConfig((options: Options): Options[] => {
  return [
    {
      entry: {
        index: "src/node/index.ts",
      },
      watch: Boolean(options.watch),
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
      watch: Boolean(options.watch),
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
      watch: Boolean(options.watch),
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
