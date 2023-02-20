const path = require("path")
const glob = require("glob")
const MiniCssExtractPlugin = require("mini-css-extract-plugin")
const CopyWebpackPlugin = require("copy-webpack-plugin")

module.exports = {
  entry: {
    "./js/app.js": ["./js/app.js"].concat(glob.sync("./vendor/**/*.js")),
  },
  output: {
    filename: "app.js",
    path: path.resolve(__dirname, "../priv/static/js"),
  },
  module: {
    rules: [
      {
        test: /\.jsx$/,
        exclude: /node_modules/,
        use: {
          loader: "babel-loader",
          options: {
            presets: [["@babel/preset-env"]],
            plugins: ["@babel/plugin-proposal-class-properties"],
          },
        },
      },
      {
        // Delete this rule when @nivo PR is merged https://github.com/plouc/nivo/pull/841
        test: /\.js$/,
        include: /node_modules\/@nivo\/bar/,
        use: {
          loader: "string-replace-loader",
          options: {
            search: "scaleBand().rangeRound(",
            replace: "scaleBand().range(",
          },
        },
      },
      {
        test: /\.(css|sass|scss)$/,
        use: [
          MiniCssExtractPlugin.loader,
          {
            loader: "css-loader",
            options: {
              url: false,
              importLoaders: 2,
              sourceMap: true,
            },
          },
          {
            loader: "postcss-loader",
            options: {
              sourceMap: true,
              postcssOptions: {
                plugins: [require("tailwindcss"), require("autoprefixer")],
              },
            },
          },
          {
            loader: "sass-loader",
            options: {
              sourceMap: true,
            },
          },
        ],
      },
      {
        test: /\.(woff(2)?|ttf|eot|svg)(\?v=\d+\.\d+\.\d+)?$/,
        use: [
          {
            loader: "file-loader",
            options: {
              name: "[name].[ext]",
              outputPath: "../fonts",
            },
          },
        ],
      },
    ],
  },
  plugins: [
    new MiniCssExtractPlugin({filename: "../css/app.css"}),

    new CopyWebpackPlugin({
      patterns: [{from: "static/", to: "../"}],
    }),
  ],
  externals: {
    jquery: "jQuery",
    lodash: "_",
    luxon: "luxon",
    react: "React",
    "react-dom": "ReactDOM",
    clipboard: "ClipboardJS",
    bootstrap: "bootstrap",
  },
}
