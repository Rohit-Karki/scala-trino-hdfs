#!/bin/bash
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

RESULTS_DIR="./results"
mkdir -p $RESULTS_DIR
ls -la $RESULTS_DIR

USERS=("analyst1" "analyst2" "data_scientist" "marketing_user" "executive_user")

declare -A QUERIES
QUERIES["analyst1"]="SELECT movie_name, COUNT(*) as views FROM hive.default.movies_dataset GROUP BY movie_name ORDER BY views DESC LIMIT 20"
QUERIES["analyst2"]="SELECT genre, AVG(CAST(rating AS DOUBLE)) as avg_rating FROM hive.default.movies_dataset GROUP BY genre ORDER BY avg_rating DESC"
QUERIES["data_scientist"]="SELECT user_id, COUNT(*) as ratings_count, AVG(CAST(rating AS DOUBLE)) as avg_rating FROM hive.default.movies_dataset GROUP BY user_id ORDER BY ratings_count DESC LIMIT 50"
QUERIES["marketing_user"]="SELECT movie_name, rating FROM hive.default.movies_dataset WHERE CAST(rating AS DOUBLE) > 4.5 ORDER BY CAST(rating AS DOUBLE) DESC LIMIT 100"
QUERIES["executive_user"]="SELECT COUNT(*) as total_ratings, COUNT(DISTINCT user_id) as unique_users FROM hive.default.movies_dataset"

run_trino_query() {
    local user=$1
    local query="${QUERIES[$user]}"
    local output_file="$(pwd)/${RESULTS_DIR}/trino_${user}_$(date +%s).out"
    
    echo -e "${YELLOW}[${user}]${NC} Executing query: ${BLUE}${query}${NC}"
    
    touch $output_file
    
    start_time=$(date +%s.%N)
    docker exec -i trino-coordinator trino \
      --server localhost:8080 \
      --catalog hive \
      --schema default \
      --user "$user" \
      --execute "$query" > $output_file 2>&1
    local status=$?
    end_time=$(date +%s.%N)
    
    execution_time=$(echo "$end_time - $start_time" | bc)
    
    if [ $status -eq 0 ]; then
        echo -e "${GREEN}[${user}]${NC} Completed in ${execution_time}s"
        echo -e "${GREEN}[${user}]${NC} Results saved to ${output_file}"
    else
        echo -e "${RED}[${user}]${NC} Query failed with status $status"
        echo -e "${RED}[${user}]${NC} Check ${output_file} for details"
    fi
    
    echo "$execution_time"
}

get_time_seconds() {
  date +%s
}

run_sequential_test() {
    echo -e "\n${GREEN}=== SCENARIO 1: Sequential Execution ===${NC}"
    echo -e "${YELLOW}======================================${NC}"
    
    local sequential_start=$(get_time_seconds)
    
    for user in "${USERS[@]}"; do
        run_trino_query "$user"
    done
    
    local sequential_end=$(get_time_seconds)
    local sequential_time=$((sequential_end - sequential_start))
    echo -e "\n${GREEN}Sequential Test Completed in ${sequential_time} seconds${NC}"
    
    echo "$sequential_time" > "$(pwd)/${RESULTS_DIR}/sequential_time.txt"
}

run_concurrent_test() {
    echo -e "\n${GREEN}=== SCENARIO 2: Concurrent Execution ===${NC}"
    echo -e "${YELLOW}======================================${NC}"
    
    local concurrent_start=$(get_time_seconds)
    
    for user in "${USERS[@]}"; do
        run_trino_query "$user" &
    done
    
    wait
    
    local concurrent_end=$(get_time_seconds)
    local concurrent_time=$((concurrent_end - concurrent_start))
    echo -e "\n${GREEN}Concurrent Test Completed in ${concurrent_time} seconds${NC}"
    
    echo "$concurrent_time" > "$(pwd)/${RESULTS_DIR}/concurrent_time.txt"
}

run_mixed_test() {
    echo -e "\n${GREEN}=== SCENARIO 3: Mixed Load with Repeated Users ===${NC}"
    echo -e "${YELLOW}======================================${NC}"
    
    local mixed_start=$(get_time_seconds)
    
    for i in {1..3}; do
        run_trino_query "analyst1" &
        run_trino_query "data_scientist" &
        sleep 1
    done

    run_trino_query "executive_user" &
    run_trino_query "marketing_user" &
    run_trino_query "analyst2" &
    
    wait
    
    local mixed_end=$(get_time_seconds)
    local mixed_time=$((mixed_end - mixed_start))
    echo -e "\n${GREEN}Mixed Test Completed in ${mixed_time} seconds${NC}"
    
    echo "$mixed_time" > "$(pwd)/${RESULTS_DIR}/mixed_time.txt"
}

print_header() {
    echo -e "\n${BLUE}============================================${NC}"
    echo -e "${BLUE}   $1   ${NC}"
    echo -e "${BLUE}============================================${NC}"
}

print_summary() {
    echo -e "\n${GREEN}=== CONCURRENCY TEST SUMMARY ===${NC}"
    echo -e "Total unique users simulated: ${#USERS[@]}"
    
    echo -e "\nExecution Times:"
    
    if [ -f "$(pwd)/${RESULTS_DIR}/sequential_time.txt" ]; then
        sequential_time=$(cat "$(pwd)/${RESULTS_DIR}/sequential_time.txt")
        echo -e " - Sequential Execution: $sequential_time seconds"
    fi
    
    if [ -f "$(pwd)/${RESULTS_DIR}/concurrent_time.txt" ]; then
        concurrent_time=$(cat "$(pwd)/${RESULTS_DIR}/concurrent_time.txt")
        echo -e " - Concurrent Execution: $concurrent_time seconds"
    fi
    
    if [ -f "$(pwd)/${RESULTS_DIR}/mixed_time.txt" ]; then
        mixed_time=$(cat "$(pwd)/${RESULTS_DIR}/mixed_time.txt")
        echo -e " - Mixed Load Execution: $mixed_time seconds"
    fi
    
    echo -e "\nQuery results:"
    for user in "${USERS[@]}"; do
        result_files=$(find "$(pwd)/${RESULTS_DIR}" -name "trino_${user}_*.out" 2>/dev/null | wc -l)
        echo -e " - User '$user': $result_files result files"
    done
}

print_header "Trino SQL Concurrency Test"

run_sequential_test
run_concurrent_test
run_mixed_test

print_summary

echo -e "\n${GREEN}All tests completed. Results are saved in $(pwd)/${RESULTS_DIR}${NC}"