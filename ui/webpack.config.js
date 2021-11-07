const CopyWebpackPlugin = require("copy-webpack-plugin");

module.exports = {
  output: {
    publicPath: "",
  },
  module: {
    rules: [
      {
        test: /\.css$/,
        use: ["style-loader", "css-loader"],
      },
    ],
  },
  plugins: [
    new CopyWebpackPlugin([{ from: "node_modules/@contiamo/code-editor/dist/editor.worker.*.js", flatten: true }]),
  ],
};
