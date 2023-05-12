import { fileURLToPath } from "url";
import path from "path";
const actualDirName = fileURLToPath(new URL(".", import.meta.url));
export default path.resolve(actualDirName, "./worker.mjs");
