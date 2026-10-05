#include "common.h"
#include <pthread.h>
#include <mqueue.h>
#include <time.h>
#include <signal.h>
#include <errno.h>

// กำหนดรหัสสี เพื่อให้ log อ่านง่ายขึ้น
#define COLOR_RESET   "\x1b[0m"
#define COLOR_RED     "\x1b[31m"
#define COLOR_GREEN   "\x1b[32m"
#define COLOR_YELLOW  "\x1b[33m"
#define COLOR_BLUE    "\x1b[34m"
#define COLOR_MAGENTA "\x1b[35m"
#define COLOR_CYAN    "\x1b[36m"

// สร้าง Array สำหรับเก็บสีของ Worker แต่ละตัว
const char* worker_colors[] = {
    COLOR_RESET,   // Index 0 (ไม่ใช้)
    COLOR_RED,     // Worker 1
    COLOR_GREEN,   // Worker 2
    COLOR_YELLOW,  // Worker 3
    COLOR_BLUE,    // Worker 4
    COLOR_MAGENTA, // Worker 5
    COLOR_CYAN     // Worker 6 เป็นต้นไป
};
 
#define AVAILABLE 0
#define RESERVED  1
 
// random delay ระหว่างขั้นตอน check และ update 
#define DELAY_MIN_MS 50
#define DELAY_MAX_MS 500

typedef struct {
    int status;  // AVAILABLE หรือ RESERVED
    int owner;   // client_id ของผู้จอง, -1 ถ้าไม่มีใครจอง */
} resource_t;

// Shared Data หลัก
static resource_t g_table[MAX_RESOURCES];
 
// Mutex 
// ป้องกัน Critical Section ของ g_table ถ้าจะใช้ต้องเปิดโหมด sync
static pthread_mutex_t g_table_mutex = PTHREAD_MUTEX_INITIALIZER;
 
// Mutex กัน worker หลายตัวพิมพ์ปนกันจนอ่านไม่รู้เรื่อง
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
 
static mqd_t g_server_mq = (mqd_t)-1;
static int g_use_sync = 1;                 // เปิด/ปิด sync เพื่อป้องกัน Race Condition

#define CS_NAME_UPDATE (g_use_sync ? "critical section" : "check-and-update region (NO LOCK)")
#define CS_NAME_READ   (g_use_sync ? "critical section" : "read region (NO LOCK)")
static volatile sig_atomic_t g_running = 1;
static long g_seq = 0;

#define WORKER_QUEUE_CAPACITY 32

typedef struct {
    Message items[WORKER_QUEUE_CAPACITY];
    int head;
    int tail;
    int count;
    int closed;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} worker_queue_t;

typedef struct {
    int worker_id;
    worker_queue_t queue;
    pthread_t thread;
} worker_context_t;

static void worker_queue_init(worker_queue_t *q) {
    memset(q, 0, sizeof(*q));
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->not_empty, NULL);
    pthread_cond_init(&q->not_full, NULL);
}

static void worker_queue_close(worker_queue_t *q) {
    pthread_mutex_lock(&q->mutex);
    q->closed = 1;
    pthread_cond_broadcast(&q->not_empty);
    pthread_cond_broadcast(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
}

static void worker_queue_destroy(worker_queue_t *q) {
    pthread_mutex_destroy(&q->mutex);
    pthread_cond_destroy(&q->not_empty);
    pthread_cond_destroy(&q->not_full);
}

static int worker_queue_push(worker_queue_t *q, const Message *msg) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == WORKER_QUEUE_CAPACITY && !q->closed && g_running) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    if (q->closed || !g_running) {
        pthread_mutex_unlock(&q->mutex);
        return 0;
    }

    q->items[q->tail] = *msg;
    q->tail = (q->tail + 1) % WORKER_QUEUE_CAPACITY;
    q->count++;

    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
    return 1;
}

static int worker_queue_pop(worker_queue_t *q, Message *out) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == 0 && !q->closed && g_running) {
        pthread_cond_wait(&q->not_empty, &q->mutex);
    }

    if (q->count == 0) {
        pthread_mutex_unlock(&q->mutex);
        return 0;
    }

    *out = q->items[q->head];
    q->head = (q->head + 1) % WORKER_QUEUE_CAPACITY;
    q->count--;

    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return 1;
}
 
// ฟังก์ชันพิมพ์ Log แบบปลอดภัย (Thread-safe)
static void log_line(const char *text) {
    pthread_mutex_lock(&g_log_mutex);
    long seq = ++g_seq;
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    printf("[seq=%04ld][t=%ld.%03ld] %s\n", seq, (long)ts.tv_sec,
           ts.tv_nsec / 1000000, text);
    fflush(stdout);
    pthread_mutex_unlock(&g_log_mutex);
}
 
// ฟังก์ชัน delay แบบสุ่ม
static void random_delay(unsigned int *seed) {
    int ms = DELAY_MIN_MS + (rand_r(seed) % (DELAY_MAX_MS - DELAY_MIN_MS + 1));
    usleep(ms * 1000);
}
 
// ฟังก์ชันส่งคำตอบกลับไปยัง client โดยเปิดคิวเฉพาะของ client_id นั้น
static void send_response(int client_id, CommandType cmd, int resource_id, ResponseStatus status, const char *text) {
    char qname[CLIENT_QUEUE_NAME_LEN];
    get_client_queue_name(client_id, qname, sizeof(qname));

    mqd_t cq = mq_open(qname, O_WRONLY);
    if (cq == (mqd_t)-1) {
        char errbuf[128];
        snprintf(errbuf, sizeof(errbuf),
                 "[Server] ERROR: เปิดคิวของ Client-%d ไม่ได้ (%s)", client_id, qname);
        log_line(errbuf);
        return;
    }
 
    Message resp;
    memset(&resp, 0, sizeof(resp));
    resp.client_id   = client_id;
    resp.cmd         = cmd;
    resp.resource_id = resource_id;
    resp.status      = status;
    strncpy(resp.message, text, sizeof(resp.message) - 1);
 
    if (mq_send(cq, (const char *)&resp, sizeof(resp), 0) == -1) {
        perror("mq_send (response)");
    }
    mq_close(cq); // ปิดแค่ descriptor ไม่ unlink เพราะไม่ใช่เจ้าของคิว
}
 
// คำสั่ง LIST: สร้างข้อความสรุปสถานะทุกที่นั่ง
static void handle_list(int worker_id, int client_id) {
    char logbuf[128];
    int cid = worker_id % 7;
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] received LIST from Client-%d",
        worker_colors[cid], worker_id, client_id);
    log_line(logbuf);
 
    char buf[256];
    int n = 0;
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
    char csbuf[128];
    snprintf(csbuf, sizeof(csbuf), "\t%s[Worker-%d] entering %s (LIST all)%s", 
        worker_colors[cid], worker_id, CS_NAME_READ, COLOR_RESET);
    log_line(csbuf);
    for (int i = 0; i < MAX_RESOURCES && n < (int)sizeof(buf) - 20; i++) {
        if (g_table[i].status == AVAILABLE) {
            n += snprintf(buf + n, sizeof(buf) - n, "[%02d: A] ", i + 1);
        } else {
            n += snprintf(buf + n, sizeof(buf) - n, "[%02d:R%d] ", i + 1, g_table[i].owner);
        }
        if ((i + 1) % 10 == 0) {
            n += snprintf(buf + n, sizeof(buf) - n, "\n");
        }
    }
    snprintf(csbuf, sizeof(csbuf), "\t%s[Worker-%d] leaving %s (LIST all)%s", 
        worker_colors[cid], worker_id, CS_NAME_READ, COLOR_RESET);
    log_line(csbuf);
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
 
    send_response(client_id, CMD_LIST, 0, RES_SUCCESS, buf);
}
 
// เช็คว่ามีคนจองที่นั่งเป้าหมายหรือยัง (มี Mutexเพื่อป้องกันการอ่านข้อมูลที่กำลังถูกแก้ไข)
static void handle_status(int worker_id, int client_id, int id) {
    char logbuf[128];
 
    if (id < 1 || id > MAX_RESOURCES) {
        send_response(client_id, CMD_STATUS, id, RES_INVALID, "invalid resource_id");
        return;
    }
    
    int cid = worker_id % 7;
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] received STATUS %d from Client-%d%s",
              worker_colors[cid], worker_id, id, client_id, COLOR_RESET);
    log_line(logbuf);
 
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] entering %s (resource %d)%s",
              worker_colors[cid], worker_id, CS_NAME_READ, id, COLOR_RESET);
    log_line(logbuf);
 
    int status = g_table[id - 1].status;
    int owner  = g_table[id - 1].owner;
 
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] leaving %s (resource %d)%s",
              worker_colors[cid], worker_id, CS_NAME_READ, id, COLOR_RESET);
    log_line(logbuf);
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
 
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] STATUS %d requested by Client-%d -> %s%s",
              worker_colors[cid], worker_id, id, client_id, status == AVAILABLE ? "AVAILABLE" : "RESERVED", COLOR_RESET);
 
    char msg[64];
    if (status == AVAILABLE) {
        snprintf(msg, sizeof(msg), "AVAILABLE"); 
    } else {
        snprintf(msg, sizeof(msg), "RESERVED (Owner: Client %d)", owner); 
    }
    send_response(client_id, CMD_STATUS, id, RES_SUCCESS, msg);
}
 
// Race Condition เกิดตรงนี้
static void handle_reserve(int worker_id, int client_id, int id, unsigned int *seed) {
    if (id < 1 || id > MAX_RESOURCES) {
        send_response(client_id, CMD_RESERVE, id, RES_INVALID, "invalid resource_id");
        return;
    }
    int idx = id - 1;
    char logbuf[128];
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] received RESERVE %d from Client-%d",
              worker_id, id, client_id);
    log_line(logbuf);
    
    // Critical Section: อ่านสถานะ + ตัดสินใจ + เขียนสถานะใหม่
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
 
    int cid = worker_id % 7;
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] entering %s (resource %d)%s", worker_colors[cid], worker_id, CS_NAME_UPDATE, id, COLOR_RESET);
    log_line(logbuf);
 
    int current_status = g_table[idx].status;
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] check Resource %d: %s%s",
              worker_colors[cid], worker_id, id, current_status == AVAILABLE ? "AVAILABLE" : "RESERVED", COLOR_RESET);
    log_line(logbuf);
 
    int success = 0;
    if (current_status == AVAILABLE) {
        // ขยาย race window ให้เห็นปัญหาชัดเจนตอนไม่มี sync
        random_delay(seed);
        g_table[idx].status = RESERVED;
        g_table[idx].owner  = client_id;
        success = 1;
    }
 
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] leaving %s (resource %d)%s", worker_colors[cid], worker_id, CS_NAME_UPDATE, id, COLOR_RESET);
    log_line(logbuf);
 
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
    // Critical Section จบ
 
    if (success) {
        snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] Resource %d reserved by Client-%d%s", worker_colors[cid], 
        worker_id, id, client_id, COLOR_RESET);
        log_line(logbuf);
        send_response(client_id, CMD_RESERVE, id, RES_SUCCESS, "RESERVE SUCCESS");
    } else {
        snprintf(logbuf, sizeof(logbuf),
                  "\t%s[Worker-%d] Resource %d already reserved (rejected Client-%d)%s", worker_colors[cid],
                  worker_id, id, client_id, COLOR_RESET);
        log_line(logbuf);
        send_response(client_id, CMD_RESERVE, id, RES_ALREADY_RESERVED, "RESERVE FAILED (already reserved)");
    }
}
 
// เช็คว่าที่นั่งเป้าหมายเป็นของผู้ที่ขอยกเลิกจริงหรือไม่ มีการล็อค Mutex ควบคุม
static void handle_cancel(int worker_id, int client_id, int id) {
    if (id < 1 || id > MAX_RESOURCES) {
        send_response(client_id, CMD_CANCEL, id, RES_INVALID, "invalid resource_id");
        return;
    }
    int idx = id - 1;
    char logbuf[128];
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] received CANCEL %d from Client-%d",
              worker_id, id, client_id);
    log_line(logbuf);
 
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
    int cid = worker_id % 7;
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] entering %s (resource %d)%s", 
        worker_colors[cid], worker_id, CS_NAME_UPDATE, id, COLOR_RESET);
    log_line(logbuf);
 
    int ok = 0;
    if (g_table[idx].status == RESERVED && g_table[idx].owner == client_id) {
        g_table[idx].status = AVAILABLE;
        g_table[idx].owner  = -1;
        ok = 1;
    }
 
    snprintf(logbuf, sizeof(logbuf), "\t%s[Worker-%d] leaving %s (resource %d)%s", worker_colors[cid], 
        worker_id, CS_NAME_UPDATE, id, COLOR_RESET);
    log_line(logbuf);
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
 
    if (ok) {
        send_response(client_id, CMD_CANCEL, id, RES_SUCCESS, "CANCEL SUCCESS");
    } else {
        send_response(client_id, CMD_CANCEL, id, RES_FAILED, "CANCEL FAILED (not your reservation)");
    }
}
 
// Worker thread: ดึงงานจาก Worker Queue ของตัวเอง
static void *worker_thread(void *arg) {
    worker_context_t *ctx = (worker_context_t *)arg;
    int worker_id = ctx->worker_id;
    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(worker_id * 7919);
    Message req;

    while (worker_queue_pop(&ctx->queue, &req)) {
        switch (req.cmd) {
            case CMD_LIST:
                handle_list(worker_id, req.client_id);
                break;
            case CMD_STATUS:
                handle_status(worker_id, req.client_id, req.resource_id);
                break;
            case CMD_RESERVE:
                handle_reserve(worker_id, req.client_id, req.resource_id, &seed);
                break;
            case CMD_CANCEL:
                handle_cancel(worker_id, req.client_id, req.resource_id);
                break;
            case CMD_QUIT: {
                char logbuf[128];
                snprintf(logbuf, sizeof(logbuf),
                         "[Worker-%d] processing QUIT from Client-%d",
                         worker_id, req.client_id);
                log_line(logbuf);
                send_response(req.client_id, CMD_QUIT, 0, RES_SUCCESS, "BYE");
                break;
            }
            default:
                send_response(req.client_id, req.cmd, req.resource_id, RES_INVALID,
                               "unknown command");
        }
    }

    char logbuf[96];
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] stopped", worker_id);
    log_line(logbuf);
    return NULL;
}

static void on_signal(int sig) {
    (void)sig;
    g_running = 0;
}

// Server Main Thread / Dispatcher
// รับ request จาก POSIX Message Queue แล้วกระจายไป Worker Queue แบบ round-robin
static int dispatch_request(worker_context_t *workers, int num_workers,
                            int *next_worker, const Message *req) {
    int worker_index = *next_worker;

    if (!worker_queue_push(&workers[worker_index].queue, req)) {
        return 0;
    }

    char logbuf[192];
    const char *cmd_name = "UNKNOWN";
    switch (req->cmd) {
        case CMD_LIST:    cmd_name = "LIST"; break;
        case CMD_STATUS:  cmd_name = "STATUS"; break;
        case CMD_RESERVE: cmd_name = "RESERVE"; break;
        case CMD_CANCEL:  cmd_name = "CANCEL"; break;
        case CMD_QUIT:    cmd_name = "QUIT"; break;
        default: break;
    }
    snprintf(logbuf, sizeof(logbuf),
             "[Dispatcher] Client-%d %s %d -> Worker-%d",
             req->client_id, cmd_name, req->resource_id,
             workers[worker_index].worker_id);
    log_line(logbuf);

    *next_worker = (*next_worker + 1) % num_workers;
    return 1;
}

int main(int argc, char *argv[]) {
    int num_workers = 3; // กำหนด default ไว้เท่ากับ 3

    if (argc >= 2) num_workers = atoi(argv[1]);
    if (num_workers < 1) num_workers = 1;

    if (argc >= 3 && strcmp(argv[2], "nosync") == 0) {
        g_use_sync = 0;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // เริ่มต้นทุก resource เป็น AVAILABLE 
    for (int i = 0; i < MAX_RESOURCES; i++) {
        g_table[i].status = AVAILABLE;
        g_table[i].owner  = -1;
    }

    mq_unlink(SERVER_QUEUE_NAME);

    struct mq_attr attr;
    attr.mq_flags   = 0;
    attr.mq_maxmsg  = MAX_MESSAGES;
    attr.mq_msgsize = MSG_SIZE;
    attr.mq_curmsgs = 0;

    g_server_mq = mq_open(SERVER_QUEUE_NAME, O_CREAT | O_RDONLY, QUEUE_PERMISSIONS, &attr);
    if (g_server_mq == (mqd_t)-1) {
        perror("mq_open (server queue)");
        return EXIT_FAILURE;
    }

    printf("============================================================================\n");
    printf(" CSS223 Cinema Reservation Server (POSIX Message Queue)\n");
    printf(" Queue      : %s\n", SERVER_QUEUE_NAME);
    printf(" Workers    : %d\n", num_workers);
    printf(" Sync mode  : %s\n", g_use_sync ? "ON (mutex enabled)" : "OFF (race demo)");
    printf(" Resources  : %d (1..%d)\n", MAX_RESOURCES, MAX_RESOURCES);
    printf(" Architecture: MQ -> Main Thread/Dispatcher -> Worker Queues -> Workers\n");
    printf("============================================================================\n");

    worker_context_t *workers = calloc((size_t)num_workers, sizeof(*workers));
    if (!workers) {
        perror("calloc workers");
        mq_close(g_server_mq);
        mq_unlink(SERVER_QUEUE_NAME);
        return EXIT_FAILURE;
    }

    int created_workers = 0;
    for (int i = 0; i < num_workers; i++) {
        workers[i].worker_id = i + 1;
        worker_queue_init(&workers[i].queue);

        if (pthread_create(&workers[i].thread, NULL, worker_thread, &workers[i]) != 0) {
            perror("pthread_create");
            g_running = 0;
            break;
        }
        created_workers++;
    }

    if (created_workers != num_workers) {
        for (int i = 0; i < created_workers; i++) {
            worker_queue_close(&workers[i].queue);
        }
        for (int i = 0; i < created_workers; i++) {
            pthread_join(workers[i].thread, NULL);
        }
        for (int i = 0; i < num_workers; i++) {
            worker_queue_destroy(&workers[i].queue);
        }
        free(workers);
        mq_close(g_server_mq);
        mq_unlink(SERVER_QUEUE_NAME);
        return EXIT_FAILURE;
    }

    // Server Main Thread / Dispatcher
    int next_worker = 0;
    while (g_running) {
        Message req;
        ssize_t n = mq_receive(g_server_mq, (char *)&req, MSG_SIZE, NULL);
        if (n == -1) {
            if (errno == EINTR) continue;
            if (!g_running) break;
            perror("mq_receive");
            continue;
        }

        if (n != (ssize_t)MSG_SIZE) {
            log_line("[Dispatcher] Ignored malformed message size");
            continue;
        }

        if (!dispatch_request(workers, num_workers, &next_worker, &req)) {
            log_line("[Dispatcher] Could not dispatch request (server shutting down)");
            break;
        }
    }
    for (int i = 0; i < num_workers; i++) {
        worker_queue_close(&workers[i].queue);
    }

    if (g_server_mq != (mqd_t)-1) {
        mq_close(g_server_mq);
        g_server_mq = (mqd_t)-1;
        mq_unlink(SERVER_QUEUE_NAME);
    }

    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i].thread, NULL);
    }

    for (int i = 0; i < num_workers; i++) {
        worker_queue_destroy(&workers[i].queue);
    }
    free(workers);

    printf("\n[Server] shutting down, message queue removed.\n");
    return EXIT_SUCCESS;
}
