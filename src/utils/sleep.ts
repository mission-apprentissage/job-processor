export async function sleep(
  durationMs: number,
  signal?: AbortSignal,
): Promise<void> {
  await new Promise<void>((resolve) => {
    let timeout: NodeJS.Timeout | null = null;

    const listener = () => {
      if (timeout) clearTimeout(timeout);
      resolve();
    };

    timeout = setTimeout(() => {
      signal?.removeEventListener("abort", listener);
      resolve();
    }, durationMs);

    signal?.addEventListener("abort", listener);
  });
}
