/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./**/*.{js,jsx,ts,tsx}",
    "../lib/logflare_web/*.{eex,leex,heex,ex}",
    "../lib/logflare_web/**/*.{eex,leex,heex,ex}",
  ],
  safelist:
    process.env.NODE_ENV === "production"
      ? undefined
      : {
          pattern: /./,
          variants: ["sm", "md", "lg", "xl", "2xl"],
        },
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
