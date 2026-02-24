/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./js/**/*.{js,jsx,ts,tsx}",
    "../lib/logflare_web/*.{eex,leex,heex,ex}",
    "../lib/logflare_web/**/*.{eex,leex,heex,ex}",
  ],
  theme: {
    extend: {
      colors: {
        "json-tree-key": "#6286db",
        "json-tree-string": "#63ff99",
        "json-tree-number": "#c7a1ff",
        "json-tree-boolean": "#fff86c",
        "json-tree-null": "#eec97d",
        "json-tree-label": "#9ca3af",
      },
    },
  },
  plugins: [],
  prefix: "tw-",
  // TODO: remove when sass is reduced.
  important: true,
  // TODO: remove once bootstrap removed (?)
  corePlugins: {
    preflight: false,
  },
};
