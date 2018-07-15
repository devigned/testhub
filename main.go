package main

import (
	"os"

	"github.com/devigned/testhub/cmd"
	"github.com/uber/jaeger-client-go"
	"github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/uber/jaeger-lib/metrics/prometheus"
)

func main() {
	if os.Getenv("TRACING") == "true" {
		// Sample configuration for testing. Use constant sampling to sample every trace
		// and enable LogSpan to log every span via configured Logger.
		cfg := config.Configuration{
			Sampler: &config.SamplerConfig{
				Type:  jaeger.SamplerTypeConst,
				Param: 1,
			},
			Reporter: &config.ReporterConfig{
				LocalAgentHostPort: "0.0.0.0:6831",
			},
		}

		// Example logger and metrics factory. Use github.com/uber/jaeger-client-go/log
		// and github.com/uber/jaeger-lib/metrics respectively to bind to real logging and metrics
		// frameworks.
		jLogger := jaegerlog.StdLogger
		metricsFactory := prometheus.New()

		closer, err := cfg.InitGlobalTracer(
			"testhub",
			config.Logger(jLogger),
			config.Metrics(metricsFactory),
		)
		if err != nil {
			panic(err)
		}
		defer closer.Close()

	}

	cmd.Execute()
}
