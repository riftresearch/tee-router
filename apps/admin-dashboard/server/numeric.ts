const DECIMAL_RE = /^\d+$/
const MAX_SAFE_INTEGER_DECIMAL_DIGITS = Number.MAX_SAFE_INTEGER.toString().length
const MAX_U256 = (1n << 256n) - 1n
const MAX_U64 = (1n << 64n) - 1n
const MAX_U256_DECIMAL_DIGITS = MAX_U256.toString().length
const MAX_U64_DECIMAL_DIGITS = MAX_U64.toString().length

export function parseNonNegativeSafeInteger(value: string, field: string): number {
  if (!DECIMAL_RE.test(value)) {
    throw new Error(`${field} must be a non-negative integer`)
  }
  if (value.length > MAX_SAFE_INTEGER_DECIMAL_DIGITS) {
    throw new Error(`${field} exceeds JavaScript safe integer range`)
  }
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed)) {
    throw new Error(`${field} exceeds JavaScript safe integer range`)
  }
  return parsed
}

export function parseU256Decimal(value: string | undefined): bigint | undefined {
  return parseBoundedNonNegativeDecimal(
    value,
    MAX_U256,
    MAX_U256_DECIMAL_DIGITS
  )
}

export function parseU64Decimal(value: string | undefined): bigint | undefined {
  return parseBoundedNonNegativeDecimal(value, MAX_U64, MAX_U64_DECIMAL_DIGITS)
}

function parseBoundedNonNegativeDecimal(
  value: string | undefined,
  maxValue: bigint,
  maxDigits: number
): bigint | undefined {
  if (!value || value.length > maxDigits || !DECIMAL_RE.test(value)) {
    return undefined
  }
  const parsed = BigInt(value)
  return parsed <= maxValue ? parsed : undefined
}
