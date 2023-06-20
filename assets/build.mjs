import * as esbuild from "esbuild";
import { sassPlugin } from "esbuild-sass-plugin";
import * as path from "path";
import { copy } from "esbuild-plugin-copy";
import postcss from "postcss";
import autoprefixer from "autoprefixer";
import tailwindcss from "tailwindcss";
import * as chokidar from "chokidar";
import tailwindConfig from "./tailwind.config.js";

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
    // copy output css to css folder
    {
      from: ["../priv/static/js/*.css"],
      to: ["../priv/static/css"],
    },
  ],
});

let sassPostcssPlugin = sassPlugin({
  async transform(source, resolveDir) {
    const { css } = await postcss([autoprefixer, tailwindcss]).process(source);
    return css;
  },
});

const options = {
  logLevel: "info",
  entryPoints: ["js/app.js"],
  bundle: true,
  minify: watch ? false : true,
  sourcemap: true,
  loader: { ".svg": "file", ".png": "file" },
  outfile: "../priv/static/js/app.js",
  plugins: [sassPostcssPlugin, externalizeCssImages, copyStatic],
  jsx: "automatic",
  treeShaking: watch ? false : true,
  nodePaths: ["node_modules"],
  color: true,
};

const printRebuildResults = (result) => {
  const toPrint = [...result.errors, ...result.warnings];
  console.log(`[CHOKIDAR] Rebuild complete`, toPrint.length > 0 ? toPrint : "");
};

if (watch) {
  const ctx = await esbuild.context(options);

  console.log(`[ESBUILD] Running build...`);
  const res = await ctx.rebuild();
  printRebuildResults(res);
  console.log("[CHOKIDAR] Watching content: ", tailwindConfig.content);
  await chokidar
    .watch(tailwindConfig.content)
    .on("change", async (event, path) => {
      console.log(`[CHOKIDAR] File change detected, triggering rebuild...`);
      const result = await ctx.rebuild({ logLevel: "info" });
      printRebuildResults(result);
    });
} else {
  console.log(`[ESBUILD] Running production build...`);
  await esbuild.build(options);
}
