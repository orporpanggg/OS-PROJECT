#include <ctype.h>
#include <errno.h>
#include <signal.h>
#include <time.h>
#include "common.h"
 
// รอคำตอบจาก server นานสุดกี่วินาที ก่อนถือว่า server ไม่ตอบ
#define REPLY_TIMEOUT_SEC 5
 
static mqd_t g_server_mq = (mqd_t)-1;
static mqd_t g_client_mq = (mqd_t)-1;
static char  g_client_queue_name[CLIENT_QUEUE_NAME_LEN];
 
// 1 = client ตัวนี้เป็นคน "สร้าง" queue สำเร็จ จึงมีสิทธิ์ unlink
static int g_queue_created = 0;
 
// ตัวแปรแฟล็กสำหรับควบคุมการหยุดการทำงานของโปรแกรมเมื่อผู้ใช้กดปิด
static volatile sig_atomic_t g_stop = 0;
 
// เช่น การกด Ctrl+C ทำหน้าที่เปลี่ยนค่า g_stop เป็น 1 เพื่อให้ลูปหลักของโปรแกรมหยุดทำงาน
static void on_signal(int sig) {
    (void)sig;
    g_stop = 1;
}
 
// ติดตั้ง handler ด้วย sigaction (ไม่ใส่ SA_RESTART)
// เพื่อให้ระบบขัดจังหวะฟังก์ชันที่กำลังบล็อกรออินพุตหรือรอข้อความให้ตื่นขึ้นมาประมวลผลการปิดโปรแกรมได้ทันที
static void setup_signals(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
}

// ฟังก์ชันสำหรับทำความสะอาดก่อนปิดโปรแกรม 
// ปิด descriptor และลบ queue ของ client ตัวเองก่อนโปรแกรมจบ
static void cleanup(void) {
    if (g_client_mq != (mqd_t)-1) {
        mq_close(g_client_mq);
        g_client_mq = (mqd_t)-1;
    }
    if (g_server_mq != (mqd_t)-1) {
        mq_close(g_server_mq);
        g_server_mq = (mqd_t)-1;
    }
    // unlink เฉพาะ queue ที่เราสร้างเองเท่านั้น (กันไปลบของ client อื่น)
    if (g_queue_created) {
        mq_unlink(g_client_queue_name);
        g_queue_created = 0;
    }
}
 
// ฟังก์ชันสำหรับล้างข้อความเก่าที่ค้างใน queue ของเราทิ้ง
// เผื่อกรณีก่อนหน้านี้ Server timeout แล้วคำตอบมาช้า มาปะปนกับผลลัพธ์ของคำสั่งปัจจุบัน
static void drain_client_queue(void) {
    struct mq_attr a;
    Message dummy;
    while (mq_getattr(g_client_mq, &a) == 0 && a.mq_curmsgs > 0) {
        if (mq_receive(g_client_mq, (char *)&dummy, MSG_SIZE, NULL) == -1) {
            break;
        }
    }
}
 
// พิมพ์เมนูคำสั่งให้ผู้ใช้ดู
static void print_menu(void) {
    printf("===========================================\n");
    printf("\nCSS223 Cinema Reservation \n");
    printf("  LIST                 - แสดงรายการที่นั่งทั้งหมด\n");
    printf("  STATUS <seat_id>     - เช็คสถานะที่นั่ง\n");
    printf("  RESERVE <seat_id>    - จองที่นั่ง\n");
    printf("  CANCEL <seat_id>     - ยกเลิกการจอง\n");
    printf("  QUIT                 - ออกจากโปรแกรม\n");
    printf("\n===========================================\n");
    printf("> ");
    fflush(stdout);
}
 
// ฟังก์ชันรับข้อความจากผู้ใช้มาแปลงให้กลายเป็น Message struct
// คืนค่า 1 = parse สำเร็จ, 0 = คำสั่งไม่ถูกต้อง (invalid)
static int parse_input(const char *line, int client_id, Message *out_msg) {
    char cmd_word[32] = {0};
    int seat_id = 0;
    int n = sscanf(line, "%31s %d", cmd_word, &seat_id); // ใช้ sscanf แยกคำสั่งตัวอักษรและหมายเลขที่นั่ง
 
    memset(out_msg, 0, sizeof(Message));
    out_msg->client_id = client_id;
    out_msg->resource_id = 0;
 
    if (n < 1) {
        return 0;
    }
 
    for (char *p = cmd_word; *p; ++p) {
        *p = (char)toupper((unsigned char)*p);
    }
 
    if (strcmp(cmd_word, "LIST") == 0) {
        out_msg->cmd = CMD_LIST;
        return 1;
    } else if (strcmp(cmd_word, "STATUS") == 0 && n == 2) {
        out_msg->cmd = CMD_STATUS;
        out_msg->resource_id = seat_id;
        return 1;
    } else if (strcmp(cmd_word, "RESERVE") == 0 && n == 2) {
        out_msg->cmd = CMD_RESERVE;
        out_msg->resource_id = seat_id;
        return 1;
    } else if (strcmp(cmd_word, "CANCEL") == 0 && n == 2) {
        out_msg->cmd = CMD_CANCEL;
        out_msg->resource_id = seat_id;
        return 1;
    } else if (strcmp(cmd_word, "QUIT") == 0) {
        out_msg->cmd = CMD_QUIT;
        return 1;
    }
 
    return 0;
}
 
int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "วิธีใช้: %s <client_id>\n", argv[0]);
        fprintf(stderr, "ตัวอย่าง: %s 1\n", argv[0]);
        return EXIT_FAILURE;
    }
 
    int client_id = atoi(argv[1]);
    if (client_id <= 0) {
        fprintf(stderr, "client_id ต้องเป็นเลขจำนวนเต็มบวก เช่น 1, 2, 3\n");
        return EXIT_FAILURE;
    }
 
    setup_signals();
 
    get_client_queue_name(client_id, g_client_queue_name, sizeof(g_client_queue_name));
 
    // สร้าง queue ของ client ตัวเองสำหรับ "รับ" คำตอบ
    struct mq_attr attr;
    attr.mq_flags = 0;
    attr.mq_maxmsg = MAX_MESSAGES;
    attr.mq_msgsize = MSG_SIZE;
    attr.mq_curmsgs = 0;
 
    // O_EXCL: ถ้ามี queue ชื่อนี้อยู่แล้วให้ล้มเหลว (ไม่ unlink ทับของเดิมอีกต่อไป)
    g_client_mq = mq_open(g_client_queue_name, O_CREAT | O_EXCL | O_RDONLY,
                           QUEUE_PERMISSIONS, &attr);
    if (g_client_mq == (mqd_t)-1) {
        if (errno == EEXIST) {
            fprintf(stderr,
                    "Client #%d ถูกใช้งานอยู่แล้ว (หรือมี queue เก่าค้าง)\n"
                    "  - ถ้าเปิดซ้ำ: ใช้ client_id อื่น\n"
                    "  - ถ้าโปรแกรมเก่าค้าง/โดน kill: rm /dev/mqueue%s\n",
                    client_id, g_client_queue_name);
        } else {
            perror("mq_open (client queue) ล้มเหลว");
        }
        return EXIT_FAILURE;
    }
    g_queue_created = 1; //สร้างคิวสำเร็จ
 
    // เปิด queue ของ server สำหรับ "ส่ง" คำสั่งไปหา 
    g_server_mq = mq_open(SERVER_QUEUE_NAME, O_WRONLY);
    if (g_server_mq == (mqd_t)-1) {
        perror("mq_open (server queue) ล้มเหลว - server เปิดอยู่หรือยัง?");
        cleanup();
        return EXIT_FAILURE;
    }
 
    printf("เชื่อมต่อสำเร็จ! คุณคือ Client #%d\n", client_id);
 
    char line[256];
    Message request;
    Message response;
    int running = 1;
    
    // การทำงานหลักของ client loop รับคำสั่งจากผู้ใช้ ส่งไปยัง server และรอรับคำตอบ
    while (running && !g_stop) {
        print_menu();

        errno = 0;
        if (fgets(line, sizeof(line), stdin) == NULL) {
            if (errno == EINTR && !g_stop) {
                clearerr(stdin);
                continue;
            }
            break;
        }
 
        line[strcspn(line, "\n")] = '\0';
 
        if (!parse_input(line, client_id, &request)) {
            printf("[Client] คำสั่งไม่ถูกต้อง กรุณาลองใหม่\n");
            continue;
        }
 
        drain_client_queue();
 
        if (mq_send(g_server_mq, (const char *)&request, MSG_SIZE, 0) == -1) {
            perror("mq_send ล้มเหลว");
            continue;
        }
        
        // timeout สำหรับรอคำตอบจาก server
        struct timespec deadline;
        clock_gettime(CLOCK_REALTIME, &deadline);
        deadline.tv_sec += REPLY_TIMEOUT_SEC;
 
        ssize_t bytes_read = mq_timedreceive(g_client_mq, (char *)&response,
                                             MSG_SIZE, NULL, &deadline);
        if (bytes_read == -1) {
            if (errno == ETIMEDOUT) {
                printf("\n[Client] server ไม่ตอบภายใน %d วินาที (server อาจปิดอยู่)\n",
                       REPLY_TIMEOUT_SEC);
            } else if (errno == EINTR) {
                if (g_stop) break;
                continue;
            } else {
                perror("mq_timedreceive ล้มเหลว");
            }
            if (request.cmd == CMD_QUIT) {
                running = 0;
            }
            continue;
        }
 
        const char *status_str;
        switch (response.status) {
            case RES_SUCCESS:          status_str = "SUCCESS";          break;
            case RES_FAILED:           status_str = "FAILED";           break;
            case RES_INVALID:          status_str = "INVALID";          break;
            case RES_ALREADY_RESERVED: status_str = "ALREADY_RESERVED"; break;
            default:                   status_str = "UNKNOWN";          break;
        }
 
        printf(">> [Client #%d] Status: %s | Message: %s\n", client_id, status_str, response.message);
 
        if (request.cmd == CMD_QUIT) {
            running = 0;
        }
    }

    cleanup();
    printf("ปิดโปรแกรมเรียบร้อย\n");
 
    return EXIT_SUCCESS;
}