// Test support: choose a Chromium that works on this machine, for every test that drives a browser.
//
// Some Chromium builds abort on any text input on some hosts (fontconfig: "SkFontMgr_FontConfigInterface
// Not implemented"). So the build is chosen by trying it: CHROMIUM_PATH if set, else each Playwright
// Chromium build that is already installed, newest first, is launched on a tiny page with a text input
// and kept only if it renders and takes typed text. This never installs a browser.
import { existsSync, readdirSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';

const PROBE_MS = 30_000;

/** The Playwright `chromium` object: an installed `playwright`, else the module PLAYWRIGHT_PATH names. */
export async function loadChromium() {
  const candidates = [];
  try { candidates.push(createRequire(import.meta.url)('playwright')); } catch { /* not installed here */ }
  if (process.env.PLAYWRIGHT_PATH) candidates.push(await import(pathToFileURL(path.resolve(process.env.PLAYWRIGHT_PATH)).href));
  for (const candidate of candidates) {
    const chromium = candidate.chromium || candidate.default?.chromium;
    if (chromium) return chromium;
  }
  throw new Error('Playwright is not installed. Install it and a Chromium once with:\n  npm install --no-save playwright && npx playwright install chromium\nor point PLAYWRIGHT_PATH at a module that exports chromium (and CHROMIUM_PATH at a Chromium binary).');
}

/** Installed Playwright Chromium builds as [{build, executable}], newest first. */
export function installedBuilds(env = process.env) {
  const roots = [env.PLAYWRIGHT_BROWSERS_PATH && env.PLAYWRIGHT_BROWSERS_PATH !== '0' ? env.PLAYWRIGHT_BROWSERS_PATH : null,
    path.join(env.XDG_CACHE_HOME || path.join(os.homedir(), '.cache'), 'ms-playwright'),
    path.join(os.homedir(), 'Library', 'Caches', 'ms-playwright'),
    path.join(env.LOCALAPPDATA || '', 'ms-playwright')].filter(Boolean);
  const found = [];
  for (const root of roots) {
    if (!existsSync(root)) continue;
    for (const name of readdirSync(root)) {
      const match = /^chromium-(\d+)$/.exec(name);
      if (!match) continue;
      const executable = ['chrome-linux64/chrome', 'chrome-linux/chrome', 'chrome-mac/Chromium.app/Contents/MacOS/Chromium', 'chrome-mac-arm64/Chromium.app/Contents/MacOS/Chromium', 'chrome-win64/chrome.exe', 'chrome-win/chrome.exe']
        .map(relative => path.join(root, name, relative)).find(existsSync);
      if (executable && !found.some(entry => entry.executable === executable)) found.push({ build: Number(match[1]), executable });
    }
  }
  return found.sort((a, b) => b.build - a.build);
}

/** Launch `executable`, load a page with a text input, type into it. Resolves to '' on success, else why not. */
async function probe(chromium, executable, args) {
  let browser;
  let timer;
  try {
    const attempt = (async () => {
      browser = await chromium.launch({ headless: true, args, ...(executable ? { executablePath: executable } : {}) });
      const page = await browser.newPage();
      await page.setContent('<!doctype html><title>probe</title><input id="t" type="text" autofocus>');
      await page.locator('#t').click();
      await page.keyboard.type('probe 123');
      const value = await page.locator('#t').inputValue();
      return value === 'probe 123' ? '' : `typed text came back as "${value}"`;
    })();
    attempt.catch(() => {}); // a late failure after the timeout is not an unhandled rejection
    return await Promise.race([attempt, new Promise(resolve => { timer = setTimeout(() => resolve(`no result in ${PROBE_MS / 1000}s`), PROBE_MS); })]);
  } catch (error) {
    const first = String(error.message || error).split('\n').find(line => line.trim()) || 'failed';
    return /closed|crash|exited/i.test(first) ? `the browser crashed on a page with a text input (${first})` : first;
  } finally {
    clearTimeout(timer);
    await browser?.close().catch(() => {});
  }
}

/**
 * Choose the Chromium to launch: `{executablePath, build, report}`. `executablePath` is undefined when
 * Playwright's own default is the one that works. `report` is the lines to print: the build chosen and
 * why each other one was skipped. Throws, naming every build tried, if none renders a text input.
 */
export async function chooseChromium(chromium, args = [], env = process.env) {
  if (env.CHROMIUM_PATH) {
    return { executablePath: env.CHROMIUM_PATH, build: 'CHROMIUM_PATH', report: [`browser: ${env.CHROMIUM_PATH} (set by CHROMIUM_PATH, not probed)`] };
  }
  const builds = installedBuilds(env);
  const skipped = [];
  for (const { build, executable } of builds) {
    const why = await probe(chromium, executable, args);
    if (!why) return { executablePath: executable, build, report: [...skipped, `browser: chromium-${build} (${executable}), the newest build that renders and takes typed text`] };
    skipped.push(`browser: skipped chromium-${build}: ${why}`);
  }
  if (!builds.length) {
    const why = await probe(chromium, undefined, args);
    if (!why) return { executablePath: undefined, build: 'playwright default', report: ['browser: Playwright\'s default Chromium (no chromium-* build found in the Playwright cache)'] };
    skipped.push(`browser: skipped Playwright's default Chromium: ${why}`);
  }
  throw new Error(`no installed Chromium renders a text input:\n  ${skipped.join('\n  ')}\nInstall one with: npx playwright install chromium (or set CHROMIUM_PATH to a Chromium binary).`);
}

/** Launch the chosen Chromium. Prints the report unless `quiet`. */
export async function launchChromium(chromium, { args = [], headless = true, quiet = false } = {}) {
  const choice = await chooseChromium(chromium, args);
  if (!quiet) for (const line of choice.report) console.log(line);
  const browser = await chromium.launch({ headless, args, ...(choice.executablePath ? { executablePath: choice.executablePath } : {}) });
  return { browser, choice };
}
