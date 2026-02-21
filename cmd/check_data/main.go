package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func main() {
	baseURL := "http://localhost:9309"

	// 1. Получаем список таблиц через SQL SELECT
	fmt.Println("\n=== SHOW TABLES (через SELECT) ===")
	resp, err := http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT * FROM information_schema.tables WHERE table_schema = 'rt' LIMIT 10")))
	if err != nil {
		log.Fatal(err)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 2. Считаем количество записей в geonames
	fmt.Println("\n=== COUNT geonames ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT COUNT(*) FROM geonames")))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 3. Считаем количество записей в hierarchy
	fmt.Println("\n=== COUNT hierarchy ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT COUNT(*) FROM hierarchy")))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 4. Проверим наличие parent_id в geonames
	fmt.Println("\n=== Проверка parent_id в geonames ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT COUNT(*) FROM geonames WHERE parent_id > 0")))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 5. Первые 5 записей из geonames с parent_id
	fmt.Println("\n=== Первые 5 geonames с parent_id ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT id, name, parent_id FROM geonames WHERE parent_id > 0 LIMIT 5")))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 6. Статистика по типам связей в hierarchy
	fmt.Println("\n=== Типы связей в hierarchy ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT relation_type, COUNT(*) FROM hierarchy GROUP BY relation_type")))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 7. Топ-10 стран по количеству записей
	fmt.Println("\n=== Топ-10 стран ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte("SELECT country_code, COUNT(*) as cnt FROM geonames GROUP BY country_code ORDER BY cnt DESC LIMIT 10")))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)

	// 8. Проверим наличие admin кодов в geonames
	fmt.Println("\n=== Примеры admin кодов ===")
	resp, err = http.Post(baseURL+"/sql", "text/plain",
		bytes.NewReader([]byte(`SELECT id, name, country_code, admin1_code, admin2_code 
        FROM geonames 
        WHERE admin1_code != '' AND admin1_code != '00' 
        LIMIT 5`)))
	if err != nil {
		log.Fatal(err)
	}
	json.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()
	printJSON(result)
}

func printJSON(v interface{}) {
	pretty, _ := json.MarshalIndent(v, "", "  ")
	fmt.Println(string(pretty))
}
