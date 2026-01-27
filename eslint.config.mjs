import { fileURLToPath } from "node:url";
import reactPlugin from "eslint-plugin-react";
import reactHooks from "eslint-plugin-react-hooks";
import { createTypeScriptImportResolver } from "eslint-import-resolver-typescript";
import { defineConfig } from "eslint/config";
import { includeIgnoreFile } from "@eslint/compat";
import globals from "globals";
import js from "@eslint/js";
import * as tseslint from "typescript-eslint";
import * as importX from "eslint-plugin-import-x";
import nodePlugin from "eslint-plugin-n";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

const gitignorePath = fileURLToPath(new URL(".gitignore", import.meta.url));

const ALL_FILES = "**/*.{js,mjs,cjs,ts,tsx,jsx}";
const TS_FILES = "**/*.{ts,tsx}";

export default defineConfig([
  includeIgnoreFile(gitignorePath),

  nodePlugin.configs["flat/recommended"],
  js.configs.recommended,
  importX.flatConfigs.recommended,
  importX.flatConfigs.typescript,
  tseslint.configs.recommended,

  {
    name: "all-files",
    files: [ALL_FILES],
    languageOptions: {
      globals: {
        ...globals.node,
      },

      ecmaVersion: "latest",
      sourceType: "module",
    },
    rules: {
      // redundant with import-x
      "n/no-missing-import": "off",
      "n/no-extraneous-import": "off",

      "import-x/no-named-as-default-member": "off",
      "import-x/default": "off",
      "import-x/order": "error",
      "import-x/no-cycle": ["error", { ignoreExternal: true }],
      "import-x/no-relative-packages": "error",
      "import-x/no-useless-path-segments": ["error"],
      "import-x/consistent-type-specifier-style": ["error", "prefer-top-level"],
      "import-x/no-extraneous-dependencies": [
        "error",
        {
          devDependencies: [
            "**/*.test.ts",
            "**/*.test.tsx",
            "**/tests/**/*.ts",
            "**/tests/*.ts",
            "**/fixtures/**/*.ts",
            "**/tsup.config.ts",
            "**/vitest.config.ts",
            "**/eslint.config.mjs",
          ],
        },
      ],
    },
    settings: {
      "import-x/resolver-next": [
        createTypeScriptImportResolver({
          project: ["tsconfig.json"],
          fullySpecified: true,
        }),
      ],
    },
  },

  {
    name: "typescript-files",
    files: [TS_FILES],
    languageOptions: {
      parserOptions: {
        project: ["tsconfig.json"],
        tsconfigRootDir: __dirname,
      },
    },
    rules: {
      "@typescript-eslint/consistent-type-imports": [
        "error",
        {
          prefer: "type-imports",
        },
      ],

      "@typescript-eslint/ban-ts-comment": ["off"],
      "@typescript-eslint/no-import-type-side-effects": "error",
      "@typescript-eslint/promise-function-async": "error",
      "@typescript-eslint/switch-exhaustiveness-check": "error",

      "@typescript-eslint/no-unused-vars": [
        "error",
        {
          args: "all",
          argsIgnorePattern: "^_",
          caughtErrors: "all",
          caughtErrorsIgnorePattern: "^_",
          destructuredArrayIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          ignoreRestSiblings: true,
        },
      ],
    },
  },
  {
    files: [["src/nodes/**", ALL_FILES]],
    languageOptions: {
      globals: globals.node,
    },
  },
  {
    files: [["src/react/**", ALL_FILES]],
    ...reactHooks.configs.flat["recommended-latest"],
  },
  {
    files: [["src/react/**", ALL_FILES]],
    ...reactPlugin.configs.flat.recommended,
    settings: {
      react: {
        version: "detect",
      },
    },
  },
  {
    files: [["src/react/**", ALL_FILES]],
    ...reactPlugin.configs.flat["jsx-runtime"],
    settings: {
      react: {
        version: "detect",
      },
    },
  },
  {
    files: [["src/react/**", ALL_FILES]],
    languageOptions: {
      globals: {
        ...globals.serviceworker,
        ...globals.browser,
      },
    },
  },
  {
    name: "ui-files",
    files: [["src/react/**", ALL_FILES]],
    rules: {
      "n/no-unpublished-import": "off",
      "react/no-unescaped-entities": "off",
    },
  },
  {
    name: "test-files",
    files: ["**/*.test.{js,mjs,cjs,ts,tsx,jsx}"],
    rules: {
      "@typescript-eslint/no-explicit-any": "off",
    },
  },
]);
