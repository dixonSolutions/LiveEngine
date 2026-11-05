// Remove inactive peers as well as old timestamps for peers that remain active.
export function pruneRates(rates, now, windowMs) {
  for (const [peer, timestamps] of rates) {
    const active = timestamps.filter(time => now - time < windowMs);
    if (active.length) rates.set(peer, active);
    else rates.delete(peer);
  }
}
