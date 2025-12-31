package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Job представляет задание для обработки URL
type Job struct {
	ID  int    // Уникальный ID задания
	URL string // URL для обработки
}

// Result представляет результат обработки задания
type Result struct {
	Job      Job    // Исходное задание
	Status   string // Статус обработки ("обработан")
	Duration string // Время выполнения
}

// ШАГ 2: Создаем функцию worker имитирует обработку HTTP-запроса к URL
// Функция принимает на вход канал для чтения заданий (<-chan Job), канал для записи результатов (chan<- Result) и *sync.WaitGroup
func worker(jobs <-chan Job, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	// цикл по каналу с заданиями
	for job := range jobs {
		// Имитация случайной задержки HTTP-запроса (100-800 мс)
		delay := time.Duration(rand.Intn(700)+100) * time.Millisecond
		time.Sleep(delay)

		// Формируем результат
		result := Result{
			Job:      job,
			Status:   "обработан",
			Duration: delay.String(),
		}
		// Отправка результата обработки в канал результатов
		results <- result
	}
}

func main() {
	// Список URL для тестирования
	urls := []string{
		"https://example.com",
		"https://google.com",
		"https://github.com",
		"https://stackoverflow.com",
		"https://golang.org",
		"https://ya.ru",
		"https://habr.com",
		"https://t.me",
		"https://vk.com",
		"https://discord.com",
		"https://yahoo.com",
	}

	// ШАГ 3: Worker Pool: создаем буферизированные каналы для заданий и результатов
	const numWorkers = 3
	const workerBuffer = 10

	jobs := make(chan Job, workerBuffer)
	results := make(chan Result, len(urls))

	// Запускаем фиксированное количество воркеров
	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(jobs, results, &wg)
	}

	// ШАГ 4: Fan-in - отправляем ВСЕ задания в канал jobs и закрываем его
	go func() {
		for id, url := range urls {
			jobs <- Job{ID: id + 1, URL: url}
		}
		close(jobs) // Закрываем канал заданий - воркеры завершатся
	}()

	// Дожидаемся завершения всех воркеров и закрываем канал results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Шаг 5: Агрегация результатов
	var allResults []Result
	totalDuration := time.Duration(0)
	successCount := 0

	// используем цикл for range по каналу results, чтобы прочитать все результаты и собираем их в слайс
	for result := range results {
		allResults = append(allResults, result)
		dur, _ := time.ParseDuration(result.Duration)
		totalDuration += dur
		if result.Status == "обработан" {
			successCount++
		}
		fmt.Printf("✅ %s: %s (%.2f сек)\n", result.Job.URL, result.Status, dur.Seconds())
	}

	// Финальный отчёт
	avgDuration := totalDuration / time.Duration(len(allResults))
	fmt.Println("\n=== ОТЧЁТ О ПРОИЗВОДИТЕЛЬНОСТИ ===")
	fmt.Printf("Всего URL: %d\n", len(urls))
	fmt.Printf("Успешно обработано: %d\n", successCount)
	fmt.Printf("Среднее время: %s (%.2f сек)\n", avgDuration, avgDuration.Seconds())
	fmt.Printf("Общее время: %s\n", totalDuration)
	fmt.Printf("Производительность: %.1f URL/сек\n",
		float64(len(urls))/avgDuration.Seconds())
}
