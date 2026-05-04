import * as esbuild from "esbuild";
import { sassPlugin } from "esbuild-sass-plugin";
import * as path from "path";
import { copy } from "esbuild-plugin-copy";
import postcss from "postcss";
import autoprefixer from "autoprefixer";
import tailwindcss from "tailwindcss";
import tailwindConfig from "./tailwind.config.js";
import { globSync } from "glob";
const watch = process.argv[2] ? true : false;

if (watch) {
  console.log("[ESBUILD] Initiating watch mode...");
}

let externalizeCssImages = {
  name: "ext-css-img",
  setup(build) {
    // Intercept import paths called "env" so esbuild doesn't attempt
    // to map them to a file system location. Tag them with the "env-ns"
    // namespace to reserve them for this plugin.
    build.onResolve({ filter: /\.png$/ }, (args) => {
      return {
        path: args.path,
        external: true,
      };
    });
  },
};

let copyStatic = copy({
  // this is equal to process.cwd(), which means we use cwd path as base path to resolve `to` path
  // if not specified, this plugin uses ESBuild.build outdir/outfile options as base path.
  resolveFrom: path.dirname("."),
  once: true,
  assets: [
    {
      from: ["./static/**/*"],
      to: ["../priv/static"],
    },
  ],
});

// needed because we want to run the sass-plugin for tailwind content files
const watchPaths = tailwindConfig.content.flatMap((pattern) => {
  return globSync(pattern, { ignore: "node_modules/**" });
});

let sassPostcssPlugin = sassPlugin({
  async transform(source, resolveDir) {
    const { css } = await postcss([autoprefixer, tailwindcss]).process(source);
    // specify the loader, otherwise plugin tries to resolve the files as js
    // https://github.com/glromeo/esbuild-sass-plugin/blob/main/src/plugin.ts#L86
    return { loader: "css", contents: css, watchFiles: watchPaths };
  },
});

const options = {
  logLevel: "info",
  entryPoints: ["js/app.js", "js/source.js"],
  bundle: true,
  minify: watch ? false : true,
  sourcemap: true,
  loader: { ".svg": "file", ".png": "file" },
  outdir: "../priv/static/js",
  plugins: [sassPostcssPlugin, externalizeCssImages, copyStatic],
  jsx: "automatic",
  treeShaking: watch ? false : true,
  nodePaths: ["node_modules"],
  color: true,
};
if (watch) {
  let ctx = await esbuild.context(options);
  await ctx.watch();
} else {
  await esbuild.build(options);
}
