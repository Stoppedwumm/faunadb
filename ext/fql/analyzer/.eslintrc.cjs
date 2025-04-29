module.exports = {
  plugins: ["@typescript-eslint", "eslint-plugin-tsdoc", "jest", "prettier"],
  env: {
    browser: true,
    es2021: true,
    node: true,
    "jest/globals": true
  },
  extends: ["eslint:recommended", "plugin:@typescript-eslint/recommended"],
  overrides: [],
  parser: "@typescript-eslint/parser",
  parserOptions: {
    project: ["./tsconfig.json", "./test/tsconfig.json"],
    tsConfigRootDir: __dirname,
    ecmaVersion: "latest",
    sourceType: "module",
  },
  rules: {
    "@typescript-eslint/no-explicit-any": ["off"],
    "tsdoc/syntax": "error",
    "no-unused-vars": "off",
    "@typescript-eslint/no-unused-vars": [
      "error",
      {
        argsIgnorePattern: "^_",
        varsIgnorePattern: "^_",
        caughtErrorsIgnorePattern: "^_",
      },
    ],
    "prettier/prettier": "error"
  },
};
