package storage

import "errors"

var (
	ERR_URL_NOT_FOUND = errors.New("Url not found")
	ERR_URL_EXISTS    = errors.New("Url already exists")
	ERR_NO_URLS_IN_DB = errors.New("There is not urls in db")
)
