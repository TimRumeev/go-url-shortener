package response

type Response struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type Url struct {
	Id    int64
	Alias string
	Url   string
}

const (
	StatusOk    = "OK"
	StatusError = "Error"
)

func OK() Response {
	return Response{
		Status: StatusOk,
	}
}

func Error(msg string) Response {
	return Response{
		Status: StatusError,
		Error:  msg,
	}
}
