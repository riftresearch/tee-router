export type ErrorCode =
  | 'AUTHENTICATION_ERROR'
  | 'BAD_GATEWAY'
  | 'CONFIGURATION_ERROR'
  | 'UPSTREAM_ERROR'
  | 'VALIDATION_ERROR'

export type GatewayErrorStatus = 400 | 401 | 500 | 502 | 503

export type GatewayErrorBody = {
  error: {
    code: ErrorCode
    message: string
    details?: unknown
  }
}

export class GatewayValidationError extends Error {
  readonly status = 400
  readonly code = 'VALIDATION_ERROR' as const

  constructor(
    message: string,
    readonly details?: unknown
  ) {
    super(message)
  }
}

export class GatewayConfigurationError extends Error {
  readonly status = 503
  readonly code = 'CONFIGURATION_ERROR' as const
}

export class GatewayAuthenticationError extends Error {
  readonly status = 401
  readonly code = 'AUTHENTICATION_ERROR' as const
}

export class UpstreamHttpError extends Error {
  readonly code = 'UPSTREAM_ERROR' as const

  constructor(
    readonly upstreamStatus: number,
    message: string,
    readonly upstreamBody: unknown
  ) {
    super(message)
  }

  publicStatus(): GatewayErrorStatus {
    return 502
  }
}

export function errorBody(
  code: ErrorCode,
  message: string,
  details?: unknown
): GatewayErrorBody {
  return {
    error: {
      code,
      message,
      ...(details === undefined ? {} : { details })
    }
  }
}

export function normalizeError(error: unknown): {
  status: GatewayErrorStatus
  body: GatewayErrorBody
} {
  if (error instanceof GatewayValidationError) {
    return {
      status: error.status,
      body: errorBody(error.code, error.message, error.details)
    }
  }

  if (error instanceof GatewayConfigurationError) {
    return {
      status: error.status,
      body: errorBody(error.code, error.message)
    }
  }

  if (error instanceof GatewayAuthenticationError) {
    return {
      status: error.status,
      body: errorBody(error.code, error.message)
    }
  }

  if (error instanceof UpstreamHttpError) {
    return {
      status: error.publicStatus(),
      body: errorBody(error.code, error.message, {
        upstreamStatus: error.upstreamStatus,
        upstreamBody: error.upstreamBody
      })
    }
  }

  const message = error instanceof Error ? error.message : 'Unexpected error'
  return {
    status: 500,
    body: errorBody('BAD_GATEWAY', message)
  }
}
