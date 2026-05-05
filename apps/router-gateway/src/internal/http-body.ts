export type LimitedResponseText = {
  text: string
  truncated: boolean
}

export async function readLimitedResponseText(
  response: Response,
  maxBytes: number
): Promise<LimitedResponseText> {
  if (!response.body) return { text: '', truncated: false }

  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let totalBytes = 0
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      totalBytes += value.byteLength
      if (totalBytes > maxBytes) {
        await reader.cancel().catch(() => undefined)
        return { text: '', truncated: true }
      }
      chunks.push(value)
    }
  } finally {
    reader.releaseLock()
  }

  return {
    text: new TextDecoder().decode(concatChunks(chunks, totalBytes)),
    truncated: false
  }
}

function concatChunks(chunks: Uint8Array[], totalBytes: number): Uint8Array {
  const output = new Uint8Array(totalBytes)
  let offset = 0
  for (const chunk of chunks) {
    output.set(chunk, offset)
    offset += chunk.byteLength
  }
  return output
}
