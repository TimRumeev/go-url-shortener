package getall

import (
	"errors"
	"log/slog"
	"net/http"

	resp "ex.com/internal/lib/api/response"
	"ex.com/internal/lib/loggeer/sl"
	"ex.com/internal/storage"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"
)

type Response struct {
	resp.Response
	Urls []resp.Url `json:"urls"`
}

type UrlGetter interface {
	GetAll() ([]resp.Url, error)
}

func New(log *slog.Logger, urlGetter UrlGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.url.GetAll"

		log = log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		result, err := urlGetter.GetAll()
		if errors.Is(err, storage.ERR_NO_URLS_IN_DB) {
			log.Warn("there is no urls in db")
			render.JSON(w, r, Response{resp.OK(), result})
			return
		}
		if err != nil {
			log.Error("failed to get all urls", sl.Err(err))
			render.JSON(w, r, resp.Error("failed to get all urls"))
			return
		}

		log.Info("All urls", slog.Any("urls", result))

		render.JSON(w, r, Response{
			Response: resp.OK(),
			Urls:     result,
		})

	}
}
