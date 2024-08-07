import esbuild from "esbuild";
import { execSync } from "child_process";

const shouldWatch = process.env.DEV == "true";
const cwd = process.cwd();

const commonDirName = `${cwd}/src/common.ts`;
const esmDirName = `${cwd}/src/esm.ts`;

const buildOptions = {
  platform: "node",
  format: "cjs",
  target: "ES6",
  bundle: true,
  minify: !shouldWatch,
  entryPoints: ["./src/index.ts", "./src/worker.ts"],
  external: ["@aws-sdk/*"],
  outdir: "dist",
  plugins: [
    {
      name: "dummy",
      setup(build) {
        const { format } = build.initialOptions;
        build.onResolve({ filter: /^resolvedPaths$/ }, (args) => {
          return {
            path: format == "esm" ? esmDirName : commonDirName,
          };
        });
        build.onEnd(() => {
          console.log("Compiler rebuild", new Date().toLocaleString());
          try {
            execSync("tsc");
          } catch (error) {
            console.log(error.output?.[1]?.toString());
          }
        });
      },
    },
  ],
};

const cjs = await esbuild[shouldWatch ? "context" : "build"](buildOptions);
const esm = await esbuild[shouldWatch ? "context" : "build"]({ ...buildOptions, format: "esm", outExtension: { ".js": ".mjs" }, target: "ES2020" });
if (shouldWatch) {
  await cjs.watch();
  await esm.watch();
}
