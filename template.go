package main

import (
	"bytes"
	"strings"
	"text/template"
)

var asynqTemplate = `
{{$svrType := .ServiceType}}
{{$svrName := .ServiceName}}
type {{.ServiceType}}TaskServer interface {
{{- range .MethodSets}}
	{{.Name}}(context.Context, *{{.Request}}) (error)
{{- end}}
}

func Register{{.ServiceType}}TaskServer(mux *asynq.ServeMux, srv {{.ServiceType}}TaskServer) {
	{{- range .Methods}}
	mux.HandleFunc("{{.Typename}}", _{{$svrType}}_{{.Name}}_Task_Handler(srv))
	{{- end}}
}

{{range .Methods}}
func _{{$svrType}}_{{.Name}}_Task_Handler(srv {{$svrType}}TaskServer) func(context.Context, *asynq.Task) error {
	return func(ctx context.Context, task *asynq.Task) error {
		in := &{{.Request}}{}

		ctx, span, err := myasynq.Handle_task_before(ctx, task, in)
		if err != nil {
			return err
		}

		err = srv.{{.Name}}(ctx, in)

		myasynq.Handle_task_after(span, err)

		return err
	}
}
{{end}}

type {{.ServiceType}}TaskClient interface {
{{- range .MethodSets}}
	{{.Name}}(ctx context.Context, req *{{.Request}}, opts ...asynq.Option) (info *asynq.TaskInfo, span oteltrace.Span, err error) 
{{- end}}
}

type {{.ServiceType}}TaskClientImpl struct{
	cc *asynq.Client
}
	
func New{{.ServiceType}}TaskClient (client *asynq.Client) {{.ServiceType}}TaskClient {
	return &{{.ServiceType}}TaskClientImpl{client}
}

{{range .MethodSets}}
func (c *{{$svrType}}TaskClientImpl) {{.Name}}(ctx context.Context, in *{{.Request}}, opts ...asynq.Option) (*asynq.TaskInfo, oteltrace.Span, error) {
	if rkgrpcctx.GetTracerPropagator(ctx) != nil {
		ctx = rkgrpcctx.InjectSpanToNewContext(ctx)
	}

	spanCtx := oteltrace.SpanContextFromContext(ctx)
	ctx, span := myasynq.HolderTracer().Start(oteltrace.ContextWithRemoteSpanContext(ctx, spanCtx), "{{.Name}}Client")
	defer span.End()

	// get trace metadata
	m := make(map[string]string)
	myasynq.HolderPropagator().Inject(ctx, propagation.MapCarrier(m))

	wrap, err := json.Marshal(myasynq.WrapPayload{
		Trace: m,
		Payload: in,
	})
	if err != nil {
		return nil, nil, err
	}

	task := asynq.NewTask("{{.Typename}}", wrap, opts...)

	info, err := c.cc.Enqueue(task)
	if err != nil {
		return nil, nil, err
	}
	return info, span, nil
}
{{end}}
`

type serviceDesc struct {
	ServiceType string // Greeter
	ServiceName string // helloworld.Greeter
	Metadata    string // api/helloworld/helloworld.proto
	Methods     []*methodDesc
	MethodSets  map[string]*methodDesc
}

type methodDesc struct {
	// method
	Name    string
	Num     int
	Request string
	Reply   string
	// asynq rule
	Typename string
}

func (s *serviceDesc) execute() string {
	s.MethodSets = make(map[string]*methodDesc)
	for _, m := range s.Methods {
		s.MethodSets[m.Name] = m
	}
	buf := new(bytes.Buffer)
	tmpl, err := template.New("asynq").Parse(strings.TrimSpace(asynqTemplate))
	if err != nil {
		panic(err)
	}
	if err := tmpl.Execute(buf, s); err != nil {
		panic(err)
	}
	return strings.Trim(buf.String(), "\r\n")
}
