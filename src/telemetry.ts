import { FastifyOtelInstrumentation } from "@fastify/otel";
import { PrometheusExporter } from "@opentelemetry/exporter-prometheus";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { HttpInstrumentation } from "@opentelemetry/instrumentation-http";
import { PgInstrumentation } from "@opentelemetry/instrumentation-pg";
import { UndiciInstrumentation } from "@opentelemetry/instrumentation-undici";
import { resourceFromAttributes } from "@opentelemetry/resources";
import { NodeSDK } from "@opentelemetry/sdk-node";

const probes = new Set(["/health", "/ready", "/api/health", "/metrics"]);
const isProbe = (url = "") => probes.has(url.split("?")[0]);

const sdk = new NodeSDK({
  resource: resourceFromAttributes({ "service.version": process.env.RELEASE ?? "local" }),
  traceExporter: new OTLPTraceExporter(),
  metricReaders: [new PrometheusExporter({ host: "0.0.0.0", port: Number(process.env.METRICS_PORT ?? 9464) })],
  instrumentations: [
    new HttpInstrumentation({ ignoreIncomingRequestHook: (request) => isProbe(request.url) }),
    new UndiciInstrumentation(),
    new PgInstrumentation({ requireParentSpan: true }),
    new FastifyOtelInstrumentation({ registerOnInitialization: true, ignorePaths: (route) => isProbe(route.url) }),
  ],
});

sdk.start();
process.once("SIGTERM", () => void sdk.shutdown());
