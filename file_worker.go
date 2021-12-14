package main

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
)

type ParseRequest struct {
	file     string
	jsonResp chan map[string]interface{}
}

type WorkerPool struct {
	wg      sync.WaitGroup
	running bool

	files chan ParseRequest
}

// get file, read
type Worker struct {
	files chan ParseRequest

	quit chan bool
}

func NewWorker(files chan ParseRequest) *Worker {
	worker := &Worker{
		files: files,
	}

	return worker
}

func (w *Worker) processFile(req ParseRequest) error {
	defer close(req.jsonResp)
	file, err := os.Open(req.file)
	if err != nil {
		log.Println("open file", err)
		return err
	}
	reader := bufio.NewReader(file)

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			log.Println("File finished", req.file)
			break
		}

		var jmap map[string]interface{}
		err = json.Unmarshal(line, &jmap)
		if err != nil {
			// invalid json -> skip
			// log.Println("invalid Json", err)
			continue
		}
		req.jsonResp <- jmap
	}

	defer file.Close()
	return nil
}

func (w *Worker) Run() {
	for {
		select {
		case fileRec := <-w.files:
			err := w.processFile(fileRec)
			if err != nil {
				log.Println(err)
			}

		case <-w.quit:
			// to stop on halt
			return
		}
	}
}

func (wp *WorkerPool) Run(max_workers int) {
	if wp.running {
		return
	}
	wp.running = true // todo: sync issue
	wp.files = make(chan ParseRequest)

	for i := 0; i < max_workers; i++ {
		wp.wg.Add(1)

		worker := NewWorker(wp.files)

		go func() {
			defer wp.wg.Done()
			worker.Run()
		}()
	}

	wp.wg.Wait()
	wp.running = false
}
