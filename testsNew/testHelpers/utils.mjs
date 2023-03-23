import { setTimeout } from "timers";

export function waitMs(ms) {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve();
    }, ms);
  });
}
