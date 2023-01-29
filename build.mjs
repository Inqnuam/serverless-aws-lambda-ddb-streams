import esbuild from "esbuild";
import { execSync } from "child_process";

const shouldWatch = process.env.DEV == "true";

const compileDeclarations = () => {
  try {
    execSync("tsc");
  } catch (error) {
    console.log(error.output?.[1]?.toString());
  }
};

const esBuildConfig = {
  bundle: true,
  minify: !shouldWatch,
  platform: "node",
  target: "es2018",
  outdir: "dist",
  format: "cjs",
  entryPoints: ["./src/index.ts", "./src/worker.ts"],
  external: ["@aws-sdk/client-dynamodb", "@aws-sdk/client-dynamodb-streams"],
  watch: shouldWatch && {
    onRebuild: () => {
      console.log("Compiler rebuild", new Date().toLocaleString());
      compileDeclarations();
    },
  },
};

const result = await esbuild.build(esBuildConfig);
compileDeclarations();
console.log(result);
