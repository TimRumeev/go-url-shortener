package redirect

import (
	"errors"
	"log/slog"
	"net/http"

	resp "ex.com/internal/lib/api/response"
	"ex.com/internal/lib/loggeer/sl"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/render"

	"ex.com/internal/storage"
)

type URLGetter interface {
	GetUrlByAlias(alias string) (string, error)
}

func New(log *slog.Logger, urlGetter URLGetter) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		const op = "handlers.url.redirect.New"

		log = log.With(
			slog.String("op", op),
			slog.String("request_id", middleware.GetReqID(r.Context())),
		)

		alias := chi.URLParam(r, "alias")
		if alias == "" {
			log.Info("no alias")
			render.JSON(w, r, resp.Error("alias not found"))

			return
		}

		resURL, err := urlGetter.GetUrlByAlias(alias)
		if errors.Is(err, storage.ERR_URL_NOT_FOUND) {
			log.Info("url not found", "alias", alias)
			render.JSON(w, r, resp.Error("url not found"))
			return
		}
		if err != nil {
			log.Error("error getting url by alias", sl.Err(err))
			render.JSON(w, r, resp.Error("something went wrong"))

			return
		}
		log.Info("got url", slog.String("url", resURL))
		http.Redirect(w, r, resURL, http.StatusFound)

	}
}
