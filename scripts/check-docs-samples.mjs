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
const typed = []; // TypeScript samples, type-checked together against the package's own declarations
const PACKAGE = '@dixonsolutions/liveengine';
// The package and its subpaths resolve to this checkout's declarations, as `exports` maps them.
const packagePaths = {
  [PACKAGE]: ['index.d.ts'],
  [`${PACKAGE}/hubs`]: ['lib/hubs.d.ts'],
  [`${PACKAGE}/live-client`]: ['lib/live-client.d.mts'],
  [`${PACKAGE}/hub-api`]: ['lib/hub-api.d.mts'],
};

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
await mkdir(path.join(output, 'ts'), { recursive: true });
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
      const name = path.join(output, 'ts', `sample_${typed.length}.ts`);
      // `export {}` keeps each sample a module of its own, so their names never collide.
      await writeFile(name, `${source}\nexport {};\n`);
      typed.push({ name, location });
    } else {
      counts.js++;
      const result = spawnSync(process.execPath, ['--check', '--input-type=module'], { input: source, encoding: 'utf8' });
      if (result.status !== 0) fail(location, result.stderr.trim());
    }
  }
}
if (typed.length) {
  // Third-party modules a sample imports (a framework, say) are declared as `any`; the package's
  // own specifiers are not, so a wrong subpath or export name fails.
  const external = new Set();
  for (const { name } of typed) {
    const file = ts.createSourceFile(name, await readFile(name, 'utf8'), ts.ScriptTarget.ESNext);
    for (const statement of file.statements) {
      const specifier = (ts.isImportDeclaration(statement) || ts.isExportDeclaration(statement)) && statement.moduleSpecifier && ts.isStringLiteral(statement.moduleSpecifier) ? statement.moduleSpecifier.text : null;
      if (specifier && !specifier.startsWith('node:') && !specifier.startsWith('.') && !specifier.startsWith(PACKAGE)) external.add(specifier);
    }
  }
  const ambient = path.join(output, 'ts', 'external.d.ts');
  await writeFile(ambient, [...external].map(specifier => `declare module ${JSON.stringify(specifier)};`).join('\n') + '\n');
  // Globals a framework recipe relies on: Nitro auto-imports these.
  const globals = path.join(output, 'ts', 'framework-globals.d.ts');
  await writeFile(globals, [
    'declare function defineEventHandler<T>(handler: (event: any) => T): (event: any) => T;',
    'declare function readBody<T = any>(event: any): Promise<T>;',
    'declare function getQuery<T = any>(event: any): T;',
    'declare function createError(input: any): Error;',
    'declare const $fetch: any;',
  ].join('\n') + '\n');
  const program = ts.createProgram([...typed.map(sample => sample.name), ambient, globals], {
    noEmit: true,
    strict: false,
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    allowImportingTsExtensions: true,
    skipLibCheck: true,
    experimentalDecorators: true,
    types: ['node'],
    typeRoots: [path.join(root, 'node_modules', '@types')],
    baseUrl: root,
    paths: packagePaths,
  });
  const byFile = new Map(typed.map(sample => [path.resolve(sample.name), sample.location]));
  for (const diagnostic of ts.getPreEmitDiagnostics(program)) {
    if (diagnostic.category !== ts.DiagnosticCategory.Error) continue;
    const message = ts.flattenDiagnosticMessageText(diagnostic.messageText, '\n');
    // A multi-file framework recipe imports the application's own files (`./liveengine.service`);
    // what it imports from them is unchecked, everything else in the sample still is.
    if (diagnostic.code === 2307 && /^Cannot find module '\.\.?\//.test(message)) continue;
    const sample = diagnostic.file && byFile.get(path.resolve(diagnostic.file.fileName));
    if (!sample) { fail('TypeScript samples', `${diagnostic.file?.fileName ?? ''} ${message}`); continue; }
    const { line } = diagnostic.file.getLineAndCharacterOfPosition(diagnostic.start ?? 0);
    const [file, start] = sample.split(':');
    fail(`${file}:${Number(start) + 1 + line}`, `TS${diagnostic.code}: ${message}`);
  }
}
if (rust.length) {
  const manifest = `[package]\nname = "liveengine-docs-samples"\nversion = "0.0.0"\nedition = "2021"\n\n[workspace]\n\n[dependencies]\nliveengine = { path = ${JSON.stringify(root)} }\nserde = { version = "1", features = ["derive"] }\nserde_json = "1"\ntokio = { version = "1", features = ["full"] }\naxum = "0.8"\nreqwest = "0.12"\nactix-web = "4"\n`;
  await writeFile(path.join(output, 'Cargo.toml'), manifest);
  const result = spawnSync('cargo', ['check', '-j', process.env.LIVEENGINE_DOCS_JOBS || '4', '--manifest-path', path.join(output, 'Cargo.toml'), '--bins'], { encoding: 'utf8', env: { ...process.env, CARGO_TARGET_DIR: process.env.CARGO_TARGET_DIR ?? path.join(root, 'target') }, maxBuffer: 20 * 1024 * 1024 });
  if (result.status !== 0) {
    let diagnostic = result.stderr || String(result.error || 'cargo check failed');
    for (const sample of rust) diagnostic = diagnostic.replaceAll(`src/bin/${sample.name}.rs`, sample.location);
    fail('Rust samples', diagnostic.trim());
  }
}
console.log(`Docs samples: ${counts.js} JS/MJS syntax-checked, ${counts.ts} TS type-checked, ${counts.rust} Rust compiled; ${counts.fragments} fragments; ${counts.rustWithoutLiveengine} Rust blocks without liveengine.`);
if (errors) process.exitCode = 1;
