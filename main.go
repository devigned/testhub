package main

import (
	"fmt"
	"os"

	_ "github.com/Azure/azure-event-hubs-go/v2"
	_ "github.com/devigned/tab/opencensus"

	"contrib.go.opencensus.io/exporter/jaeger"
	"go.opencensus.io/trace"

	"github.com/devigned/testhub/cmd"
)

func main() {
	if os.Getenv("TRACING") == "true" {
		closer, err := initOpenCensus()
		if err != nil {
			fmt.Println(err)
			return
		}
		defer closer()
	}

	cmd.Execute()
}

func initOpenCensus() (func(), error) {
	exporter, err := jaeger.NewExporter(jaeger.Options{
		AgentEndpoint:     "localhost:6831",
		CollectorEndpoint: "http://localhost:14268/api/traces",
		Process: jaeger.Process{
			ServiceName: "testhub",
		},
	})

	if err != nil {
		return nil, err
	}

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	trace.RegisterExporter(exporter)
	return exporter.Flush, nil
}

//func initOpenTracing() (func(), error){
//	// Sample configuration for testing. Use constant sampling to sample every trace
//	// and enable LogSpan to log every span via configured Logger.
//	cfg := config.Configuration{
//		Sampler: &config.SamplerConfig{
//			Type:  jaeger.SamplerTypeConst,
//			Param: 1,
//		},
//		Reporter: &config.ReporterConfig{
//			LocalAgentHostPort: "0.0.0.0:6831",
//		},
//	}
//
//	closer, err := cfg.InitGlobalTracer("testhub", config.Logger(jaegerlog.StdLogger))
//	if err != nil {
//		panic(err)
//	}
//
//	defer func(){
//		if err := closer.Close(); err != nil {
//			fmt.Println("Error: ", err)
//		}
//	}()
//}
