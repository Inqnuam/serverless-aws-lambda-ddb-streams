import esbuild from "esbuild";
import { execSync } from "child_process";

const shouldWatch = process.env.DEV == "true";

const ctx = await esbuild[shouldWatch ? "context" : "build"]({
  platform: "node",
  format: "cjs",
  target: "ES6",
  bundle: true,
  minify: !shouldWatch,
  entryPoints: ["./src/index.ts", "./src/worker.ts"],
  external: ["@aws-sdk/client-dynamodb", "@aws-sdk/client-dynamodb-streams"],
  outdir: "dist",
  plugins: [
    {
      name: "dummy",
      setup(build) {
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
});

if (shouldWatch) {
  await ctx.watch();
}
