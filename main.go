package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
)

const MAX_WORKERS = 6

var dirFlag = flag.String("dir", "./files", "Dir to scan for files")

type App struct {
	folder string

	workerPool WorkerPool
}

func InitApp() *App {
	app := &App{}
	app.folder = *dirFlag
	// run worker

	app.workerPool = WorkerPool{}
	go app.workerPool.Run(MAX_WORKERS)

	return app
}

func SendError(w http.ResponseWriter, err error) {
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// todo: add Context
func (app *App) CountHandler(w http.ResponseWriter, r *http.Request) {

	field_name, field_value, err := extractParams(r.URL.Query())
	if err != nil {
		SendError(w, err)
		return
	}

	files, err := app.ReadFiles()
	if err != nil {
		SendError(w, err)
		return
	}
	log.Println("files count", len(files))

	resuls := make(chan int64, len(files))

	var total int64
	for _, file := range files {
		req := ParseRequest{
			file:     file,
			jsonResp: make(chan map[string]interface{}),
		}

		app.workerPool.files <- req

		go func(file string) {
			var subTotal int64
			for {
				m, ok := <-req.jsonResp
				if !ok {
					break
				}
				if v, found := m[field_name]; found {
					if v.(string) == field_value {
						subTotal += 1
					}
				}
			}
			log.Println("File", file, " count", subTotal)
			resuls <- subTotal
		}(file)
	}

	for i := 0; i < len(files); i++ {
		v, ok := <-resuls
		if !ok {
			break
		}
		log.Println(".")
		total += v
	}

	fmt.Fprintf(w, "%d", total)
}

func (app *App) ReadFiles() ([]string, error) {
	files, err := ioutil.ReadDir(app.folder)
	if err != nil {
		return nil, err
	}
	var onlyFiles []string
	for i := range files {
		if files[i].IsDir() {
			continue
		}
		onlyFiles = append(onlyFiles, filepath.Join(app.folder, files[i].Name()))
	}
	return onlyFiles, nil
}

func extractParams(u url.Values) (string, string, error) {
	// todo: validation
	field_name := u.Get("field_name")
	field_value := u.Get("field_value")
	if field_name == "" {
		return "", "", errors.New("field_name is empty")
	}
	return field_name, field_value, nil
}

func main() {

	flag.Parse()

	app := InitApp()

	http.HandleFunc("/count", app.CountHandler)
	log.Println("Listen on http://localhost:3000/")
	http.ListenAndServe(":3000", nil)
}
