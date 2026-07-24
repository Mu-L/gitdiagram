import { randomUUID } from "node:crypto";

import { registerActiveGeneration } from "./cancellation";
import {
  consumeGenerationRateLimit,
  getGenerationRateLimitMessage,
  refundGenerationRateLimit,
} from "./rate-limit";
import { parseGenerateRequest } from "./types";
import { getClientIp } from "~/server/http/client-ip";
import { resolveRequestCredentials } from "~/server/http/request-credentials";

interface AdmittedGenerationRequest {
  username: string;
  repo: string;
  apiKey?: string;
  githubPat?: string;
  sessionId: string;
  cancelToken?: string;
  cancellationRegistered: boolean;
}

type GenerationRequestAdmission =
  | { admitted: true; value: AdmittedGenerationRequest }
  | { admitted: false; response: Response };

function jsonError(
  body: { error: string; errorCode: string },
  init: { status: number; retryAfterSeconds?: number },
): Response {
  return Response.json(
    {
      ok: false,
      error: body.error,
      error_code: body.errorCode,
    },
    {
      status: init.status,
      headers: {
        "Cache-Control": "no-store",
        "X-Content-Type-Options": "nosniff",
        ...(init.retryAfterSeconds
          ? { "Retry-After": String(init.retryAfterSeconds) }
          : {}),
      },
    },
  );
}

export async function admitGenerationRequest(
  request: Request,
): Promise<GenerationRequestAdmission> {
  const parsed = await parseGenerateRequest(request);
  if (!parsed.success) {
    return {
      admitted: false,
      response: jsonError(
        {
          error: parsed.error,
          errorCode: parsed.errorCode,
        },
        { status: parsed.status },
      ),
    };
  }

  const {
    username,
    repo,
    session_id: requestedSessionId,
    cancel_token: cancelToken,
  } = parsed.data;
  const { apiKey, githubPat } = await resolveRequestCredentials(request, {
    apiKey: parsed.data.api_key,
    githubPat: parsed.data.github_pat,
  });
  const rateLimitedClientIp = apiKey?.trim() ? null : getClientIp(request);
  const refundRateLimit = () =>
    refundGenerationRateLimit({ clientIp: rateLimitedClientIp });

  if (!apiKey?.trim()) {
    const rateLimit = await consumeGenerationRateLimit({
      clientIp: rateLimitedClientIp,
    });
    if (!rateLimit.allowed) {
      return {
        admitted: false,
        response: jsonError(
          {
            error: getGenerationRateLimitMessage(rateLimit.retryAfterSeconds),
            errorCode: "RATE_LIMITED",
          },
          {
            status: 429,
            retryAfterSeconds: rateLimit.retryAfterSeconds,
          },
        ),
      };
    }
  }

  const sessionId = requestedSessionId ?? randomUUID();
  let cancellationRegistered = false;
  if (requestedSessionId && cancelToken) {
    try {
      cancellationRegistered = await registerActiveGeneration(
        sessionId,
        cancelToken,
      );
    } catch {
      console.error(
        JSON.stringify({
          event: "generate.cancellation.registration_failed",
          session_id: sessionId,
          error: "Cancellation registration is temporarily unavailable.",
        }),
      );
      await refundRateLimit();
      return {
        admitted: false,
        response: jsonError(
          {
            error: "Generation is temporarily unavailable. Please retry.",
            errorCode: "CANCELLATION_UNAVAILABLE",
          },
          { status: 503 },
        ),
      };
    }

    if (!cancellationRegistered) {
      await refundRateLimit();
      return {
        admitted: false,
        response: jsonError(
          {
            error: "Generation session already exists. Please retry.",
            errorCode: "SESSION_CONFLICT",
          },
          { status: 409 },
        ),
      };
    }
  }

  return {
    admitted: true,
    value: {
      username,
      repo,
      apiKey,
      githubPat,
      sessionId,
      cancelToken,
      cancellationRegistered,
    },
  };
}
