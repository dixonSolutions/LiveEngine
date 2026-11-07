import { readdir, readFile, mkdir, rm, writeFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import ts from 'typescript';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.join(root, '.cache', 'docs-samples');
const counts = { js: 0, ts: 0, rust: 0, fragments: 0, rustWithoutLiveengine: 0 };
let errors = 0;
const rust = [];

async function files(dir) {
  const entries = await readdir(dir, { withFileTypes: true });
  return (await Promise.all(entries.map(entry => {
    const name = path.join(dir, entry.name);
    return entry.isDirectory() ? files(name) : entry.name.endsWith('.mdx') ? [name] : [];
  }))).flat().sort();
}
function fail(location, message) {
  errors++;
  console.error(`${location}: ${message}`);
}
await rm(output, { recursive: true, force: true });
await mkdir(path.join(output, 'src', 'bin'), { recursive: true });
for (const file of await files(path.join(root, 'docs-site'))) {
  const text = await readFile(file, 'utf8');
  const fence = /^ {0,3}(`{3,}|~{3,})([^\n]*)\r?\n([\s\S]*?)^ {0,3}\1\s*$/gm;
  for (const match of text.matchAll(fence)) {
    const [language, ...attributes] = match[2].trim().split(/\s+/);
    if (!['js', 'mjs', 'ts', 'rust'].includes(language)) continue;
    const line = text.slice(0, match.index).split('\n').length;
    const location = `${path.relative(root, file)}:${line}`;
    if (attributes.includes('fragment')) { counts.fragments++; continue; }
    const source = match[3];
    if (language === 'rust') {
      if (!/\bliveengine\b/.test(source)) { counts.rustWithoutLiveengine++; continue; }
      const name = `sample_${rust.length}`;
      rust.push({ name, location });
      await writeFile(path.join(output, 'src', 'bin', `${name}.rs`), source);
      counts.rust++;
    } else if (language === 'ts') {
      counts.ts++;
      const result = ts.transpileModule(source, { fileName: 'sample.ts', reportDiagnostics: true, compilerOptions: { target: ts.ScriptTarget.ESNext, module: ts.ModuleKind.ESNext } });
      for (const diagnostic of result.diagnostics ?? []) {
        if (diagnostic.category === ts.DiagnosticCategory.Error) fail(location, ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n'));
      }
    } else {
      counts.js++;
      const result = spawnSync(process.execPath, ['--check', '--input-type=module'], { input: source, encoding: 'utf8' });
      if (result.status !== 0) fail(location, result.stderr.trim());
    }
  }
}
if (rust.length) {
  const manifest = `[package]\nname = "liveengine-docs-samples"\nversion = "0.0.0"\nedition = "2021"\n\n[workspace]\n\n[dependencies]\nliveengine = { path = ${JSON.stringify(root)} }\nserde = { version = "1", features = ["derive"] }\nserde_json = "1"\ntokio = { version = "1", features = ["full"] }\naxum = "0.8"\nreqwest = "0.12"\nactix-web = "4"\n`;
  await writeFile(path.join(output, 'Cargo.toml'), manifest);
  const result = spawnSync('cargo', ['check', '--manifest-path', path.join(output, 'Cargo.toml'), '--bins'], { encoding: 'utf8', env: { ...process.env, CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR ?? path.join(root, 'target') }, maxBuffer: 20 * 1024 * 1024 });
  if (result.status !== 0) {
    let diagnostic = result.stderr || String(result.error || 'cargo check failed');
    for (const sample of rust) diagnostic = diagnostic.replaceAll(`src/bin/${sample.name}.rs`, sample.location);
    fail('Rust samples', diagnostic.trim());
  }
}
console.log(`Docs samples: ${counts.js} JS/MJS, ${counts.ts} TS, ${counts.rust} Rust compiled; ${counts.fragments} fragments; ${counts.rustWithoutLiveengine} Rust blocks without liveengine.`);
if (errors) process.exitCode = 1;
