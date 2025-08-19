/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./js/*.{js,jsx,ts,tsx}",
    "../lib/logflare_web/*.{eex,leex,heex,ex}",
    "../lib/logflare_web/**/*.{eex,leex,heex,ex}",
  ],
  theme: {
    extend: {},
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
