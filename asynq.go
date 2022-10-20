package main

import (
	"fmt"

	asynq "github.com/linchao0815/protoc-gen-go-asynqgen/proto"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

const (
	contextPackage = protogen.GoImportPath("context")
	asynqPackage   = protogen.GoImportPath("github.com/hibiken/asynq")
	emptyPackage   = protogen.GoImportPath("google.golang.org/protobuf/types/known/emptypb")
	protoPackage   = protogen.GoImportPath("google.golang.org/protobuf/proto")
	jsonPackage    = protogen.GoImportPath("encoding/json")
	traceHolder    = `
var (
	noopTracerProvider = oteltrace.NewNoopTracerProvider()
	tHolder = newTraceHolder(nil)
)

const (
	spanKey       = "SpanKey"
	traceIdKey    = "TraceIdKey"
	tracerKey     = "TracerKey"
	propagatorKey = "PropagatorKey"
	providerKey   = "ProviderKey"
)

// trace
func RegisterTraceHolder(conf *TraceConfig) {
	tHolder = newTraceHolder(conf)
}

type TraceConfig struct {
	Asynq struct {
		Trace struct {
			Enabled        bool
			ServiceName    string
			ServiceVersion string
			Exporter       struct {
				File struct {
					Enabled    bool
					OutputPath string
				}
				Jaeger struct {
					Agent struct {
						Enabled bool
						Host    string
						Port    int
					}
					Collector struct {
						Enabled  bool
						Endpoint string
						Username string
						Password string
					}
				}
			}
		}
	}
}

func newTraceHolder(conf *TraceConfig) *traceHolder {
	if conf == nil {
		conf = &TraceConfig{}
		conf.Asynq.Trace.Enabled = true
	}

	mid := &traceHolder{}

	opts := toOptions(conf)

	for i := range opts {
		opts[i](mid)
	}

	if mid.exporter == nil {
		mid.exporter = newNoopExporter()
	}

	if mid.processor == nil {
		mid.processor = sdktrace.NewBatchSpanProcessor(mid.exporter)
	}

	if mid.provider == nil {
		mid.provider = sdktrace.NewTracerProvider(
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithSpanProcessor(mid.processor),
			sdktrace.WithResource(
				sdkresource.NewWithAttributes(
					semconv.SchemaURL,
					attribute.String("service.name", conf.Asynq.Trace.ServiceName),
					attribute.String("service.version", conf.Asynq.Trace.ServiceVersion),
				)),
		)
	}

	mid.tracer = mid.provider.Tracer(conf.Asynq.Trace.ServiceName, oteltrace.WithInstrumentationVersion(contrib.SemVersion()))

	if mid.propagator == nil {
		mid.propagator = propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{})
	}

	return mid
}

type traceHolder struct {
	exporter   sdktrace.SpanExporter
	processor  sdktrace.SpanProcessor
	provider   *sdktrace.TracerProvider
	propagator propagation.TextMapPropagator
	tracer     oteltrace.Tracer
}

func GetSpan(ctx context.Context) oteltrace.Span {
	if v := ctx.Value(spanKey); v != nil {
		if res, ok := v.(oteltrace.Span); ok {
			return res
		}
	}

	_, span := noopTracerProvider.Tracer("rk-trace-noop").Start(ctx, "noop-span")
	return span
}

func GetTraceId(ctx context.Context) string {
	if v := ctx.Value(traceIdKey); v != nil {
		if res, ok := v.(string); ok {
			return res
		}
	}
	return ""
}

func GetTracer(ctx context.Context) oteltrace.Tracer {
	if v := ctx.Value(tracerKey); v != nil {
		if res, ok := v.(oteltrace.Tracer); ok {
			return res
		}
	}

	return noopTracerProvider.Tracer("rk-trace-noop")
}

func GetPropagator(ctx context.Context) propagation.TextMapPropagator {
	if v := ctx.Value(propagatorKey); v != nil {
		if res, ok := v.(propagation.TextMapPropagator); ok {
			return res
		}
	}

	return nil
}

func GetProvider(ctx context.Context) *sdktrace.TracerProvider {
	if v := ctx.Value(providerKey); v != nil {
		if res, ok := v.(*sdktrace.TracerProvider); ok {
			return res
		}
	}

	return nil
}

func NewSpan(ctx context.Context, name string) (context.Context, oteltrace.Span) {
	return GetTracer(ctx).Start(ctx, name)
}

func EndSpan(span oteltrace.Span, success bool) {
	if success {
		span.SetStatus(otelcodes.Ok, otelcodes.Ok.String())
	}

	span.End()
}

func InjectSpanToHttpReq(ctx context.Context, req *http.Request) {
	if req == nil {
		return
	}

	newCtx := oteltrace.ContextWithRemoteSpanContext(req.Context(), GetSpan(ctx).SpanContext())
	GetPropagator(ctx).Inject(newCtx, propagation.HeaderCarrier(req.Header))
}

func InjectSpanToNewContext(ctx context.Context) context.Context {
	newCtx := oteltrace.ContextWithRemoteSpanContext(ctx, GetSpan(ctx).SpanContext())
	md := metadata.Pairs()
	GetPropagator(ctx).Inject(newCtx, &grpcMetadataCarrier{Md: &md})
	newCtx = metadata.NewOutgoingContext(newCtx, md)

	return newCtx
}

// toOptions convert BootConfig into Option list
func toOptions(config *TraceConfig) []option {
	opts := make([]option, 0)

	if config.Asynq.Trace.Enabled {
		var exporter sdktrace.SpanExporter

		if config.Asynq.Trace.Exporter.File.Enabled {
			exporter = newFileExporter(config.Asynq.Trace.Exporter.File.OutputPath)
		}

		if config.Asynq.Trace.Exporter.Jaeger.Agent.Enabled {
			opts := make([]jaeger.AgentEndpointOption, 0)
			if len(config.Asynq.Trace.Exporter.Jaeger.Agent.Host) > 0 {
				opts = append(opts,
					jaeger.WithAgentHost(config.Asynq.Trace.Exporter.Jaeger.Agent.Host))
			}
			if config.Asynq.Trace.Exporter.Jaeger.Agent.Port > 0 {
				opts = append(opts, jaeger.WithAgentPort(fmt.Sprintf("%d", config.Asynq.Trace.Exporter.Jaeger.Agent.Port)))
			}

			exporter = newJaegerExporter(jaeger.WithAgentEndpoint(opts...))
		}

		if config.Asynq.Trace.Exporter.Jaeger.Collector.Enabled {
			opts := []jaeger.CollectorEndpointOption{
				jaeger.WithUsername(config.Asynq.Trace.Exporter.Jaeger.Collector.Username),
				jaeger.WithPassword(config.Asynq.Trace.Exporter.Jaeger.Collector.Password),
			}

			if len(config.Asynq.Trace.Exporter.Jaeger.Collector.Endpoint) > 0 {
				opts = append(opts, jaeger.WithEndpoint(config.Asynq.Trace.Exporter.Jaeger.Collector.Endpoint))
			}

			exporter = newJaegerExporter(jaeger.WithCollectorEndpoint(opts...))
		}

		opts = append(opts, withExporter(exporter))
	}

	return opts
}

// Option is used while creating middleware as param
type option func(*traceHolder)

// WithExporter Provide sdktrace.SpanExporter.
func withExporter(exporter sdktrace.SpanExporter) option {
	return func(opt *traceHolder) {
		if exporter != nil {
			opt.exporter = exporter
		}
	}
}

// ***************** Global *****************

// NoopExporter noop
type noopExporter struct{}

// ExportSpans handles export of SpanSnapshots by dropping them.
func (nsb *noopExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }

// Shutdown stops the exporter by doing nothing.
func (nsb *noopExporter) Shutdown(context.Context) error { return nil }

// NewNoopExporter create a noop exporter
func newNoopExporter() sdktrace.SpanExporter {
	return &noopExporter{}
}

// NewFileExporter create a file exporter whose default output is stdout.
func newFileExporter(outputPath string, opts ...stdouttrace.Option) sdktrace.SpanExporter {
	if opts == nil {
		opts = make([]stdouttrace.Option, 0)
	}

	if outputPath == "" {
		outputPath = "stdout"
	}

	if outputPath == "stdout" {
		opts = append(opts, stdouttrace.WithPrettyPrint())
	} else {
		// init lumberjack logger
		writer := rklogger.NewLumberjackConfigDefault()
		if !path.IsAbs(outputPath) {
			wd, _ := os.Getwd()
			outputPath = path.Join(wd, outputPath)
		}

		writer.Filename = outputPath

		opts = append(opts, stdouttrace.WithWriter(writer))
	}

	exporter, _ := stdouttrace.New(opts...)

	return exporter
}

// NewJaegerExporter Create jaeger exporter with bellow condition.
//
// 1: If no option provided, then export to jaeger agent at localhost:6831
// 2: Jaeger agent
//    If no jaeger agent host was provided, then use localhost
//    If no jaeger agent port was provided, then use 6831
// 3: Jaeger collector
//    If no jaeger collector endpoint was provided, then use http://localhost:14268/api/traces
func newJaegerExporter(opt jaeger.EndpointOption) sdktrace.SpanExporter {
	// Assign default jaeger agent endpoint which is localhost:6831
	if opt == nil {
		opt = jaeger.WithAgentEndpoint()
	}

	exporter, err := jaeger.New(opt)

	if err != nil {
		rkentry.ShutdownWithError(err)
	}

	return exporter
}

type grpcMetadataCarrier struct {
	Md *metadata.MD
}

// Get value with key from grpc metadata.
func (carrier *grpcMetadataCarrier) Get(key string) string {
	values := carrier.Md.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// Set value with key into grpc metadata.
func (carrier *grpcMetadataCarrier) Set(key string, value string) {
	carrier.Md.Set(key, value)
}

// Keys List keys in grpc metadata.
func (carrier *grpcMetadataCarrier) Keys() []string {
	out := make([]string, 0, len(*carrier.Md))
	for key := range *carrier.Md {
		out = append(out, key)
	}
	return out
}
`
	taskHandler = `
func _handle_task_before(ctx context.Context, task *asynq.Task, in interface{}) (context.Context, oteltrace.Span, error) {
	wrap := &wrapPayload{
		Payload: in,
		Trace: map[string]string{},
	}
	if err := json.Unmarshal(task.Payload(), &wrap); err != nil {
		return ctx, nil, fmt.Errorf("%s req=%s err=%s", task.Type(), wrap, err)
	}

	ctx = tHolder.propagator.Extract(ctx, propagation.MapCarrier(wrap.Trace))
	spanCtx := oteltrace.SpanContextFromContext(ctx)

	// create new span
	ctx, span := tHolder.tracer.Start(oteltrace.ContextWithRemoteSpanContext(ctx, spanCtx), task.Type())
	defer span.End()

	ctx = context.WithValue(ctx, spanKey, span)
	ctx = context.WithValue(ctx, traceIdKey, span.SpanContext().TraceID().String())
	ctx = context.WithValue(ctx, tracerKey, tHolder.tracer)
	ctx = context.WithValue(ctx, propagatorKey, tHolder.propagator)
	ctx = context.WithValue(ctx, providerKey, tHolder.provider)

	md := metadata.Pairs()
	tHolder.propagator.Inject(ctx, &grpcMetadataCarrier{Md: &md})
	ctx = metadata.NewOutgoingContext(ctx, md)

	return ctx, span, nil
}

func _handle_task_after(span oteltrace.Span, err error) {
	if err != nil {
		span.SetStatus(codes.Error, fmt.Sprintf("%v", err))
	} else {
		span.SetStatus(codes.Ok, "success")
	}
}
`
	wrapPayload = "type wrapPayload struct {\n\tTrace map[string]string `json:\"trace\"`\n\tPayload interface{} `json:\",inline\"`\n}"
)

var methodSets = make(map[string]int)

// generateFile generates a _asynq.pb.go file.
func generateFile(gen *protogen.Plugin, file *protogen.File) *protogen.GeneratedFile {
	if len(file.Services) == 0 || (!hasAsynqRule(file.Services)) {
		return nil
	}
	filename := file.GeneratedFilenamePrefix + "_asynq.pb.go"
	g := gen.NewGeneratedFile(filename, file.GoImportPath)
	g.P("// Code generated by protoc-gen-go-asynqgen. DO NOT EDIT.")
	g.P("// versions:")
	g.P(fmt.Sprintf("// protoc-gen-go-asynqgen %s", release))
	g.P()
	g.P("package ", file.GoPackageName)
	g.P()
	generateFileContent(gen, file, g)
	return g
}

// generateFileContent generates the kratos errors definitions, excluding the package statement.
func generateFileContent(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile) {
	if len(file.Services) == 0 {
		return
	}
	desc := `
	
	import (
		"myasynq"
		"go.opentelemetry.io/otel/propagation"
		oteltrace "go.opentelemetry.io/otel/trace"
		rkgrpcctx "github.com/rookie-ninja/rk-grpc/v2/middleware/context"		
	)
`
	g.P(desc)
	g.P("// This is a compile-time assertion to ensure that this generated file")
	g.P("// is compatible with the asynq package it is being compiled against.")
	g.P("var _ = new(", contextPackage.Ident("Context"), ")")
	g.P("var _ = new(", asynqPackage.Ident("Task"), ")")
	g.P("var _ = new(", emptyPackage.Ident("Empty"), ")")
	g.P("var _ = new(", protoPackage.Ident("Message"), ")")
	g.P("var _ = new(", jsonPackage.Ident("InvalidUTF8Error"), ")")
	//g.P(traceHolder)
	//g.P(taskHandler)
	//g.P(wrapPayload)
	for _, service := range file.Services {
		genService(gen, file, g, service)
	}
}

func genService(gen *protogen.Plugin, file *protogen.File, g *protogen.GeneratedFile, service *protogen.Service) {
	if service.Desc.Options().(*descriptorpb.ServiceOptions).GetDeprecated() {
		g.P("//")
		g.P(deprecationComment)
	}
	// Job Server.
	sd := &serviceDesc{
		ServiceType: service.GoName,
		ServiceName: string(service.Desc.FullName()),
		Metadata:    file.Desc.Path(),
	}
	for _, method := range service.Methods {
		if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
			continue
		}
		rule, ok := proto.GetExtension(method.Desc.Options(), asynq.E_Task).(*asynq.Task)
		if rule != nil && ok {
			sd.Methods = append(sd.Methods, buildMethodDesc(g, method, rule.Typename))
		}
	}
	if len(sd.Methods) != 0 {
		g.P(sd.execute())
	}
}

func hasAsynqRule(services []*protogen.Service) bool {
	for _, service := range services {
		for _, method := range service.Methods {
			if method.Desc.IsStreamingClient() || method.Desc.IsStreamingServer() {
				continue
			}
			rule, ok := proto.GetExtension(method.Desc.Options(), asynq.E_Task).(*asynq.Task)
			if rule != nil && ok {
				return true
			}
		}
	}
	return false
}

func buildMethodDesc(g *protogen.GeneratedFile, m *protogen.Method, t string) *methodDesc {
	return &methodDesc{
		Name:     m.GoName,
		Num:      methodSets[m.GoName],
		Request:  g.QualifiedGoIdent(m.Input.GoIdent),
		Reply:    g.QualifiedGoIdent(m.Output.GoIdent),
		Typename: t,
	}
}

const deprecationComment = "// Deprecated: Do not use."
