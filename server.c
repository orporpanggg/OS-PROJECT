#include "common.h"
#include <pthread.h>
#include <mqueue.h>
#include <time.h>
#include <signal.h>
#include <errno.h>
 
#define AVAILABLE 0
#define RESERVED  1
 
/* random delay ระหว่างขั้นตอน check และ update (มิลลิวินาที) */
#define DELAY_MIN_MS 50
#define DELAY_MAX_MS 500
 
/* ===================================================================
 * SHARED DATA  (ตรงกับ "Shared Reservation Table" ในรูป)
 * -------------------------------------------------------------------
 * ตัวแปรนี้คือข้อมูลกลางที่ Worker thread ทุกตัวเข้าถึงพร้อมกันได้
 * ต้นเหตุของ Race Condition ทั้งหมดอยู่ที่นี่ — Critical Section คือ
 * ช่วงที่ "อ่านสถานะ + ตัดสินใจ + เขียนสถานะใหม่" ของทรัพยากรแต่ละชิ้น
 * =================================================================== */
typedef struct {
    int status;  /* AVAILABLE หรือ RESERVED */
    int owner;   /* client_id ของผู้จอง, -1 ถ้าไม่มีใครจอง */
} resource_t;
 
static resource_t g_table[MAX_RESOURCES];
 
/* Mutex ป้องกัน Critical Section ของ g_table (ตรงกับ "Mutex/Semaphore" ในรูป)
 * เปิด/ปิดได้ด้วย g_use_sync เพื่อสาธิต Experiment 2 (ปิด) กับ 3 (เปิด) */
static pthread_mutex_t g_table_mutex = PTHREAD_MUTEX_INITIALIZER;
 
/* mutex แยกไว้ใช้ตอน printf log กัน worker หลายตัวพิมพ์ปนกันจนอ่านไม่รู้เรื่อง
 * (ไม่เกี่ยวกับ critical section ของข้อมูลจอง แค่ทำให้ log อ่านง่ายขึ้น) */
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;
 
static mqd_t g_server_mq = (mqd_t)-1;
static int g_use_sync = 1;                 /* 1=ป้องกัน race, 0=ไม่ป้องกัน (สาธิต) */
static volatile sig_atomic_t g_running = 1;
static long g_seq = 0;                     /* running sequence number สำหรับ log */
 
/* ---------- helper: log แบบมี sequence number กันบรรทัดสลับกัน ---------- */
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
 
/* ---------- random delay ระหว่าง check กับ update (เพื่อการทดลองเท่านั้น) ---------- */
static void random_delay(unsigned int *seed) {
    int ms = DELAY_MIN_MS + (rand_r(seed) % (DELAY_MAX_MS - DELAY_MIN_MS + 1));
    usleep(ms * 1000);
}
 
/* ---------- ส่งคำตอบกลับไปยัง client โดยเปิดคิวเฉพาะของ client_id นั้น ---------- */
static void send_response(int client_id, CommandType cmd, int resource_id,
                           ResponseStatus status, const char *text) {
    char qname[CLIENT_QUEUE_NAME_LEN];
    get_client_queue_name(client_id, qname, sizeof(qname));
 
    /* client ต้องเปิดคิวตัวเองไว้ล่วงหน้าแล้ว (ฝั่ง client จะทำก่อนส่ง request)
     * server แค่ "เปิดต่อ" แบบ write-only ไปส่งของ ไม่ใช่ผู้สร้างคิวนี้ */
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
    mq_close(cq); /* ปิดแค่ descriptor ของเรา ไม่ unlink เพราะไม่ใช่เจ้าของคิว */
}
 
/* ---------- คำสั่ง LIST: สร้างข้อความสรุปสถานะทุกที่นั่งแบบย่อ ---------- */
static void handle_list(int worker_id, int client_id) {
    char logbuf[128];
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] received LIST from Client-%d",
              worker_id, client_id);
    log_line(logbuf);
 
    char buf[256];
    int n = 0;
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
    char csbuf[64];
    snprintf(csbuf, sizeof(csbuf), "[Worker-%d] entering critical section (LIST all)", worker_id);
    log_line(csbuf);
    for (int i = 0; i < MAX_RESOURCES && n < (int)sizeof(buf) - 12; i++) {
        if (g_table[i].status == AVAILABLE) {
            n += snprintf(buf + n, sizeof(buf) - n, "%d:A ", i + 1);
        } else {
            n += snprintf(buf + n, sizeof(buf) - n, "%d:R(%d) ", i + 1, g_table[i].owner);
        }
    }
    snprintf(csbuf, sizeof(csbuf), "[Worker-%d] leaving critical section (LIST all)", worker_id);
    log_line(csbuf);
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
 
    send_response(client_id, CMD_LIST, 0, RES_SUCCESS, buf);
}
 
/* ---------- คำสั่ง STATUS <id> ---------- */
static void handle_status(int worker_id, int client_id, int id) {
    char logbuf[128];
 
    if (id < 1 || id > MAX_RESOURCES) {
        send_response(client_id, CMD_STATUS, id, RES_INVALID, "invalid resource_id");
        return;
    }
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] received STATUS %d from Client-%d",
              worker_id, id, client_id);
    log_line(logbuf);
 
    /* STATUS เป็นแค่การอ่านค่า (read-only) แต่ก็ยังต้องล็อกเหมือนกัน เพราะ
     * worker ตัวอื่นอาจกำลังเขียนทับ g_table[idx] อยู่พอดี (เช่น RESERVE)
     * ถ้าอ่านโดยไม่ล็อก อาจได้ค่าที่เขียนไปครึ่งเดียว (torn read) */
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] entering critical section (resource %d)",
              worker_id, id);
    log_line(logbuf);
 
    int status = g_table[id - 1].status;
    int owner  = g_table[id - 1].owner;
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] leaving critical section (resource %d)",
              worker_id, id);
    log_line(logbuf);
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] STATUS %d requested by Client-%d -> %s",
              worker_id, id, client_id, status == AVAILABLE ? "AVAILABLE" : "RESERVED");
    log_line(logbuf);
 
    char msg[64];
    snprintf(msg, sizeof(msg), "%s owner=%d",
             status == AVAILABLE ? "AVAILABLE" : "RESERVED", owner);
    send_response(client_id, CMD_STATUS, id, RES_SUCCESS, msg);
}
 
/* ---------- คำสั่ง RESERVE <id>  : จุดเกิด Race Condition ---------- */
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
 
    /* =========================================================
     * CRITICAL SECTION เริ่มต้นที่นี่ (ถ้า g_use_sync = 1)
     * ครอบตั้งแต่ "check" ไปจนถึง "update" ทั้งก้อน เพื่อไม่ให้ worker
     * สองตัวเห็นสถานะ AVAILABLE พร้อมกันแล้วต่างคนต่างเขียนทับกัน
     * ========================================================= */
    if (g_use_sync) pthread_mutex_lock(&g_table_mutex);
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] entering critical section (resource %d)",
              worker_id, id);
    log_line(logbuf);
 
    int current_status = g_table[idx].status;
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] check Resource %d: %s",
              worker_id, id, current_status == AVAILABLE ? "AVAILABLE" : "RESERVED");
    log_line(logbuf);
 
    int success = 0;
    if (current_status == AVAILABLE) {
        /* ขยาย race window ให้เห็นปัญหาชัดเจนตอนไม่มี sync — ใช้เพื่อการทดลองเท่านั้น
         * ไม่ใช่ส่วนหนึ่งของระบบจริง (ตามที่โจทย์ระบุในข้อ 3) */
        random_delay(seed);
        g_table[idx].status = RESERVED;
        g_table[idx].owner  = client_id;
        success = 1;
    }
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] leaving critical section (resource %d)",
              worker_id, id);
    log_line(logbuf);
 
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
    /* ========================= CRITICAL SECTION จบ ========================= */
 
    if (success) {
        snprintf(logbuf, sizeof(logbuf), "[Worker-%d] Resource %d reserved by Client-%d",
                  worker_id, id, client_id);
        log_line(logbuf);
        send_response(client_id, CMD_RESERVE, id, RES_SUCCESS, "RESERVE SUCCESS");
    } else {
        snprintf(logbuf, sizeof(logbuf),
                  "[Worker-%d] Resource %d already reserved (rejected Client-%d)",
                  worker_id, id, client_id);
        log_line(logbuf);
        send_response(client_id, CMD_RESERVE, id, RES_FAILED, "RESERVE FAILED (already reserved)");
    }
}
 
/* ---------- คำสั่ง CANCEL <id> ---------- */
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
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] entering critical section (resource %d)",
              worker_id, id);
    log_line(logbuf);
 
    int ok = 0;
    if (g_table[idx].status == RESERVED && g_table[idx].owner == client_id) {
        g_table[idx].status = AVAILABLE;
        g_table[idx].owner  = -1;
        ok = 1;
    }
 
    snprintf(logbuf, sizeof(logbuf), "[Worker-%d] leaving critical section (resource %d)",
              worker_id, id);
    log_line(logbuf);
    if (g_use_sync) pthread_mutex_unlock(&g_table_mutex);
 
    if (ok) {
        send_response(client_id, CMD_CANCEL, id, RES_SUCCESS, "CANCEL SUCCESS");
    } else {
        send_response(client_id, CMD_CANCEL, id, RES_FAILED, "CANCEL FAILED (not your reservation)");
    }
}
 
/* ---------- Worker thread หลัก: วนดึงงานจากคิวไปทำทีละใบ ---------- */
static void *worker_thread(void *arg) {
    int worker_id = *(int *)arg;
    unsigned int seed = (unsigned int)time(NULL) ^ (unsigned int)(worker_id * 7919);
    Message req;
 
    while (g_running) {
        /* mq_receive ปลอดภัยเมื่อถูกเรียกพร้อมกันจากหลาย thread บน descriptor
         * เดียวกัน เพราะ kernel เป็นผู้ล็อกคิวให้เองภายใน (atomic dequeue) */
        ssize_t n = mq_receive(g_server_mq, (char *)&req, MSG_SIZE, NULL);
        if (n == -1) {
            if (errno == EINTR) continue;
            if (!g_running) break; /* คิวถูกปิด/ลบระหว่าง shutdown */
            perror("mq_receive");
            continue;
        }
 
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
                char logbuf[64];
                snprintf(logbuf, sizeof(logbuf), "[Worker-%d] Client-%d QUIT",
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
    return NULL;
}
 
/* ---------- ปิดระบบอย่างปลอดภัยเมื่อกด Ctrl+C ---------- */
static void cleanup_and_exit(int sig) {
    (void)sig;
    g_running = 0;
    if (g_server_mq != (mqd_t)-1) {
        mq_close(g_server_mq);
        mq_unlink(SERVER_QUEUE_NAME); /* server เป็นเจ้าของคิวนี้ ต้อง unlink เอง */
    }
    printf("\n[Server] shutting down, message queue removed.\n");
    _exit(0); /* ใช้ _exit เพราะเรียกจาก signal handler ไม่ควรใช้ exit() ธรรมดา */
}
 
int main(int argc, char *argv[]) {
    int num_workers = 3; /* โจทย์กำหนดขั้นต่ำ 3 worker */
 
    if (argc >= 2) num_workers = atoi(argv[1]);
    if (num_workers < 1) num_workers = 1;
 
    if (argc >= 3 && strcmp(argv[2], "nosync") == 0) {
        g_use_sync = 0;
    }
 
    signal(SIGINT, cleanup_and_exit);
 
    /* เริ่มต้นทุกทรัพยากรเป็น AVAILABLE */
    for (int i = 0; i < MAX_RESOURCES; i++) {
        g_table[i].status = AVAILABLE;
        g_table[i].owner  = -1;
    }
 
    /* ลบคิวเก่าที่อาจค้างจากการรันครั้งก่อน (สำคัญมากตอนทดสอบซ้ำหลายรอบ
       เพราะถ้าคิวเก่ายังมี mq_open(O_CREAT) จะไปเปิดของเก่าทันทีโดยไม่รีเซ็ต) */
    mq_unlink(SERVER_QUEUE_NAME); /* ไม่ต้องเช็ค error เพราะถ้าไม่มีอยู่แล้วก็ไม่เป็นไร */
 
    struct mq_attr attr;
    attr.mq_flags   = 0;
    attr.mq_maxmsg  = MAX_MESSAGES;
    attr.mq_msgsize = MSG_SIZE;
    attr.mq_curmsgs = 0;
 
    g_server_mq = mq_open(SERVER_QUEUE_NAME, O_CREAT | O_RDONLY, QUEUE_PERMISSIONS, &attr);
    if (g_server_mq == (mqd_t)-1) {
        perror("mq_open (server queue)");
        exit(1);
    }
 
    printf("=========================================================\n");
    printf(" CSS223 Cinema Reservation Server (POSIX Message Queue)\n");
    printf(" Queue      : %s\n", SERVER_QUEUE_NAME);
    printf(" Workers    : %d\n", num_workers);
    printf(" Sync mode  : %s\n", g_use_sync ? "ON (mutex enabled)" : "OFF (race demo)");
    printf(" Resources  : %d (1..%d)\n", MAX_RESOURCES, MAX_RESOURCES);
    printf("=========================================================\n");
 
    pthread_t *threads = malloc(sizeof(pthread_t) * num_workers);
    int *ids = malloc(sizeof(int) * num_workers);
    for (int i = 0; i < num_workers; i++) {
        ids[i] = i + 1;
        if (pthread_create(&threads[i], NULL, worker_thread, &ids[i]) != 0) {
            perror("pthread_create");
            exit(1);
        }
    }
    for (int i = 0; i < num_workers; i++) {
        pthread_join(threads[i], NULL);
    }
    free(threads);
    free(ids);
    return 0;
}