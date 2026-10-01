/** MongoDB duplicate-key error (E11000). */
export function isDuplicateKeyError(err: unknown): boolean {
  if (typeof err !== "object" || err === null) return false;
  const code = (err as { code?: unknown }).code;
  if (code === 11000 || code === 11001) return true;
  const message = (err as { message?: unknown }).message;
  return typeof message === "string" && message.includes("E11000");
}
