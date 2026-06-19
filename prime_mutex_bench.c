#define _GNU_SOURCE
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    uint64_t n;
    uint64_t chunk_size;
    uint64_t next_start;
    uint64_t total_primes;
    pthread_mutex_t work_mutex;
    pthread_mutex_t total_mutex;
} Shared;

typedef struct {
    Shared *shared;
    uint64_t local_primes;
} WorkerArg;

static void die_pthread(int rc, const char *msg) {
    if (rc != 0) {
        fprintf(stderr, "%s: %s\n", msg, strerror(rc));
        exit(EXIT_FAILURE);
    }
}

static double now_sec(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        perror("clock_gettime");
        exit(EXIT_FAILURE);
    }
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
}

static int is_prime(uint64_t x) {
    if (x < 2) return 0;
    if (x == 2 || x == 3) return 1;
    if (x % 2 == 0 || x % 3 == 0) return 0;

    for (uint64_t d = 5; d <= x / d; d += 6) {
        if (x % d == 0 || x % (d + 2) == 0) return 0;
    }
    return 1;
}

static uint64_t count_primes_sequential(uint64_t n) {
    uint64_t count = 0;

    for (uint64_t x = 2; x <= n; x++) {
        count += (uint64_t)is_prime(x);
    }

    return count;
}

static void *worker(void *arg) {
    WorkerArg *wa = (WorkerArg *)arg;
    Shared *s = wa->shared;
    uint64_t local = 0;

    for (;;) {
        uint64_t start, end;

        /*
         * 여러 스레드가 동시에 s->next_start를 가져가면 안 된다.
         * 그래서 work_mutex로 보호한다.
         */
        die_pthread(pthread_mutex_lock(&s->work_mutex),
                    "pthread_mutex_lock(work_mutex)");

        start = s->next_start;

        if (start > s->n) {
            die_pthread(pthread_mutex_unlock(&s->work_mutex),
                        "pthread_mutex_unlock(work_mutex)");
            break;
        }

        s->next_start += s->chunk_size;

        die_pthread(pthread_mutex_unlock(&s->work_mutex),
                    "pthread_mutex_unlock(work_mutex)");

        end = start + s->chunk_size - 1;
        if (end > s->n || end < start) {
            end = s->n;
        }

        /*
         * 여기서는 mutex를 쓰지 않는다.
         * local은 각 스레드 자기 변수라서 공유 데이터가 아니다.
         */
        for (uint64_t x = start; x <= end; x++) {
            local += (uint64_t)is_prime(x);
        }
    }

    wa->local_primes = local;

    /*
     * 전체 결과 total_primes는 공유 데이터이므로 mutex로 보호한다.
     * 단, 매번 소수를 찾을 때마다 lock하지 않고,
     * 스레드가 자기 일을 끝낸 뒤 딱 한 번만 더한다.
     */
    die_pthread(pthread_mutex_lock(&s->total_mutex),
                "pthread_mutex_lock(total_mutex)");

    s->total_primes += local;

    die_pthread(pthread_mutex_unlock(&s->total_mutex),
                "pthread_mutex_unlock(total_mutex)");

    return NULL;
}

static uint64_t count_primes_parallel_mutex(uint64_t n,
                                            int thread_count,
                                            uint64_t chunk_size) {
    Shared shared;

    shared.n = n;
    shared.chunk_size = chunk_size;
    shared.next_start = 2;
    shared.total_primes = 0;

    die_pthread(pthread_mutex_init(&shared.work_mutex, NULL),
                "pthread_mutex_init(work_mutex)");

    die_pthread(pthread_mutex_init(&shared.total_mutex, NULL),
                "pthread_mutex_init(total_mutex)");

    pthread_t *threads = calloc((size_t)thread_count, sizeof(*threads));
    WorkerArg *args = calloc((size_t)thread_count, sizeof(*args));

    if (threads == NULL || args == NULL) {
        perror("calloc");
        exit(EXIT_FAILURE);
    }

    for (int i = 0; i < thread_count; i++) {
        args[i].shared = &shared;
        args[i].local_primes = 0;

        die_pthread(pthread_create(&threads[i], NULL, worker, &args[i]),
                    "pthread_create");
    }

    for (int i = 0; i < thread_count; i++) {
        die_pthread(pthread_join(threads[i], NULL), "pthread_join");
    }

    uint64_t result = shared.total_primes;

    die_pthread(pthread_mutex_destroy(&shared.work_mutex),
                "pthread_mutex_destroy(work_mutex)");

    die_pthread(pthread_mutex_destroy(&shared.total_mutex),
                "pthread_mutex_destroy(total_mutex)");

    free(threads);
    free(args);

    return result;
}

static uint64_t parse_u64(const char *s, const char *name) {
    errno = 0;

    char *end = NULL;
    unsigned long long v = strtoull(s, &end, 10);

    if (errno != 0 || end == s || *end != '\0') {
        fprintf(stderr, "invalid %s: %s\n", name, s);
        exit(EXIT_FAILURE);
    }

    return (uint64_t)v;
}

static int parse_int(const char *s, const char *name) {
    uint64_t v = parse_u64(s, name);

    if (v == 0 || v > 1024) {
        fprintf(stderr, "%s must be between 1 and 1024\n", name);
        exit(EXIT_FAILURE);
    }

    return (int)v;
}

int main(int argc, char **argv) {
    uint64_t n = 5000000ULL;

    long cpu_count = sysconf(_SC_NPROCESSORS_ONLN);
    int thread_count = (cpu_count > 0 && cpu_count < 1024)
                           ? (int)cpu_count
                           : 4;

    uint64_t chunk_size = 1000ULL;

    if (argc >= 2) {
        n = parse_u64(argv[1], "N");
    }

    if (argc >= 3) {
        thread_count = parse_int(argv[2], "thread_count");
    }

    if (argc >= 4) {
        chunk_size = parse_u64(argv[3], "chunk_size");
    }

    if (chunk_size == 0) {
        fprintf(stderr, "chunk_size must be greater than 0\n");
        return EXIT_FAILURE;
    }

    printf("Count primes from 2 to %" PRIu64 "\n", n);
    printf("Threads: %d, chunk size: %" PRIu64 "\n\n",
           thread_count, chunk_size);

    double t1 = now_sec();
    uint64_t seq = count_primes_sequential(n);
    double t2 = now_sec();

    double t3 = now_sec();
    uint64_t par = count_primes_parallel_mutex(n, thread_count, chunk_size);
    double t4 = now_sec();

    double seq_time = t2 - t1;
    double par_time = t4 - t3;

    printf("Sequential result     : %" PRIu64 " primes, %.6f sec\n",
           seq, seq_time);

    printf("Pthread+mutex result  : %" PRIu64 " primes, %.6f sec\n",
           par, par_time);

    if (seq != par) {
        printf("ERROR: results differ!\n");
        return EXIT_FAILURE;
    }

    if (par_time > 0.0) {
        printf("Speedup              : %.2fx\n", seq_time / par_time);
    }

    return EXIT_SUCCESS;
}