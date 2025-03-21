package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
)

var (
	host     = os.Getenv("DB_HOST")
	port     = os.Getenv("DB_PORT")
	user     = os.Getenv("DB_USER")
	password = os.Getenv("DB_PASSWORD")
	dbname   = os.Getenv("DB_NAME")
)

type Handlers struct {
	dbProvider DatabaseProvider
	jobQueue   *JobQueue
}

type DatabaseProvider struct {
	db *sql.DB
}

type Job struct {
	ID       int         `json:"id"`
	Name     string      `json:"name"`
	Data     interface{} `json:"data"`
	Status   string      `json:"status"`
	Priority int         `json:"priority"`
}

type JobQueue struct {
	jobs       chan Job
	maxWorkers int
	retryLimit int
	dbProvider DatabaseProvider
}

func NewJobQueue(maxWorkers, queueSize, retryLimit int, dbProvider DatabaseProvider) *JobQueue {
	return &JobQueue{
		jobs:       make(chan Job, queueSize),
		maxWorkers: maxWorkers,
		retryLimit: retryLimit,
		dbProvider: dbProvider,
	}
}

func (jq *JobQueue) StartWorkers() {
	for i := 0; i < jq.maxWorkers; i++ {
		go jq.worker(i)
	}
}

func (jq *JobQueue) worker(workerID int) {
	for job := range jq.jobs {
		retries := 0
		for retries < jq.retryLimit {
			err := PerformJob(job.Name, job.Data)
			if err == nil {
				break
			}
			retries++
			log.Printf("Worker %d retrying job %s (attempt %d)\n", workerID, job.Name, retries)
			time.Sleep(time.Second * time.Duration(retries)) // Exponential backoff
		}
		if retries == jq.retryLimit {
			log.Printf("Worker %d failed job %s after %d retries\n", workerID, job.Name, retries)
			err := jq.dbProvider.UpdateJobStatus(job.Name, "failed")
			if err != nil {
				log.Printf("Error updating status for job %s: %v\n", job.Name, err)
			}
		} else {
			err := jq.dbProvider.UpdateJobStatus(job.Name, "finished")
			if err != nil {
				log.Printf("Error updating status for job %s: %v\n", job.Name, err)
			}
		}
	}
}

func PerformJob(name string, jobData interface{}) error {
	fmt.Printf("Started job %s at %d\n", name, time.Now().UnixMilli())
	time.Sleep(time.Duration(rand.Int63n(int64(3 * time.Second))))
	if rand.Intn(2) == 0 {
		return fmt.Errorf("random error")
	}
	fmt.Printf("Finished job %s at %d\n", name, time.Now().UnixMilli())
	return nil
}

func (h *Handlers) CreateJob(w http.ResponseWriter, r *http.Request) {
	var job struct {
		Name     string      `json:"name"`
		Data     interface{} `json:"data"`
		Priority int         `json:"priority"`
	}

	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&job)
	if err != nil {
		http.Error(w, "Ошибка JSON", http.StatusBadRequest)
		return
	}

	if job.Name == "" {
		http.Error(w, "Имя задачи обязательно", http.StatusBadRequest)
		return
	}

	// Вставляем задачу в базу данных и получаем её ID
	jobID, err := h.dbProvider.InsertJob(job.Name, job.Data, job.Priority)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	// Добавляем задачу в очередь на выполнение
	h.jobQueue.jobs <- Job{
		ID:       jobID,
		Name:     job.Name,
		Data:     job.Data,
		Priority: job.Priority,
		Status:   "pending",
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(fmt.Sprintf("Задача %s создана с ID %d", job.Name, jobID)))
}

func (dp *DatabaseProvider) InsertJob(name string, data interface{}, priority int) (int, error) {
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return 0, err
	}

	// Вставляем задачу в базу данных
	_, err = dp.db.Exec(
		`INSERT INTO jobs (name, data, priority, status, created_at, updated_at)
		   VALUES ($1, $2, $3, 'pending', NOW(), NOW())`, name, dataJSON, priority)
	if err != nil {
		return 0, err
	}

	// Получаем ID последней вставленной задачи
	var jobID int
	err = dp.db.QueryRow("SELECT id FROM jobs WHERE name = $1", name).Scan(&jobID)
	if err != nil {
		return 0, err
	}

	return jobID, nil
}

func (dp *DatabaseProvider) UpdateJobStatus(name, status string) error {
	if dp.db != nil {
		err := dp.db.Ping()
		if err != nil {
			return fmt.Errorf("ошибка базы данных: %v", err)
		}
	} else {
		return fmt.Errorf("не установлено соединение с базой данных")
	}

	_, err := dp.db.Exec(
		`UPDATE jobs SET status = $1, updated_at = NOW() WHERE name = $2`, status, name)
	return err
}
func (h *Handlers) GetJobStatus(w http.ResponseWriter, r *http.Request) {
	job_id := r.URL.Path[len("/jobs/"):]

	if job_id == "" {
		http.Error(w, "id задачи не указано", http.StatusBadRequest)
		return
	}

	status, err := h.dbProvider.SelectJobStatus(job_id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("Ошибка получения статуса задачи: %v", err)))
		return
	}

	if status == "Не найдена" {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf("Задача с id %s не найдена", job_id)))
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Статус задачи %s: %s", job_id, status)))
}

func (dp *DatabaseProvider) SelectJobStatus(jobID string) (string, error) {
	var status string
	row := dp.db.QueryRow("SELECT status FROM jobs WHERE id = $1", jobID)
	err := row.Scan(&status)
	if err != nil {
		if err == sql.ErrNoRows {
			return "Не найдена", nil
		} else {
			return "", err
		}
	}
	return status, nil
}

func main() {
	address := flag.String("address", "0.0.0.0:8081", "адрес для запуска сервера")
	flag.Parse()

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	dp := DatabaseProvider{db: db}
	jobQueue := NewJobQueue(5, 100, 3, dp) // 5 воркеров, очередь на 100 задач, 3 ретрая
	jobQueue.StartWorkers()

	h := Handlers{dbProvider: dp, jobQueue: jobQueue}

	http.HandleFunc("/jobs", h.CreateJob)
	http.HandleFunc("/jobs/", h.GetJobStatus)

	server := &http.Server{Addr: *address}

	// Graceful shutdown
	go func() {
		sigchan := make(chan os.Signal, 1)
		signal.Notify(sigchan, os.Interrupt, syscall.SIGTERM)
		<-sigchan

		log.Println("Shutting down server...")
		server.Shutdown(context.Background())
	}()

	log.Printf("Server started at %s\n", *address)
	err = server.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
