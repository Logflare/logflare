// @ts-check
// Note: type annotations allow type checking and IDEs autocompletion

const { themes } = require("prism-react-renderer");
const lightCodeTheme = themes.github;
const darkCodeTheme = themes.dracula;

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: "Logflare Docs",
  tagline: "Never get surprised by a logging bill again",
  url: "https://docs.logflare.com",
  baseUrl: "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "warn",
  favicon: "img/favicon.ico",

  // GitHub pages deployment config.
  // If you aren't using GitHub pages, you don't need these.
  organizationName: "Logflare", // Usually your GitHub org/user name.
  projectName: "logflare", // Usually your repo name.

  // Even if you don't use internalization, you can use this field to set useful
  // metadata like html lang. For example, if your site is Chinese, you may want
  // to replace "en" with "zh-Hans".
  i18n: {
    defaultLocale: "en",
    locales: ["en"],
  },

  presets: [
    [
      "classic",
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        blog: false,
        docs: {
          routeBasePath: "/",
          sidebarPath: require.resolve("./sidebars.js"),
          // Please change this to your repo.
          // Remove this to remove the "edit this page" links.
          editUrl:
            "https://github.com/Logflare/logflare/tree/staging/docs/docs.logflare.com",
        },
        theme: {
          customCss: require.resolve("./src/css/custom.scss"),
        },
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      tableOfContents: {
        minHeadingLevel: 2,
        maxHeadingLevel: 5,
      },
      navbar: {
        title: "Logflare Docs",
        hideOnScroll: true,
        logo: {
          alt: "Logflare",
          src: "img/apple-icon.png",
        },
        items: [
          {
            href: "https://logflare.app/dashboard",
            label: "dashboard",
            position: "left",
          },
          {
            href: "https://logflare.app/swaggerui",
            label: "OpenAPI",
            position: "right",
          },
          {
            href: "https://github.com/Logflare/logflare",
            label: "GitHub",
            position: "right",
          },
        ],
      },
      footer: {
        style: "dark",
        links: [
          {
            title: "Service",
            items: [
              {
                label: "logflare.app",
                href: "https://logflare.app",
              },
              {
                label: "GitHub",
                href: "https://github.com/Logflare/logflare",
              },
              {
                label: "Supabase",
                href: "https://supabase.com",
              },
            ],
          },
        ],
        copyright: `Copyright © ${new Date().getFullYear()} Logflare, part of Supabase`,
      },
      prism: {
        theme: lightCodeTheme,
        darkTheme: darkCodeTheme,
      },
    }),

  plugins: ["docusaurus-plugin-sass"],
};

module.exports = config;
