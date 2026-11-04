'use strict';
const { readFileSync } = require('node:fs');
const version = require('../package.json').version;
const cargoVersion = readFileSync('Cargo.toml', 'utf8').match(/^version = "([^"]+)"/m)?.[1];
if (cargoVersion !== version) throw new Error('Cargo and npm versions must match');
const tag = process.env.RELEASE_TAG;
if (tag && tag !== `v${version}`) throw new Error(`Release tag ${tag} does not match v${version}`);
