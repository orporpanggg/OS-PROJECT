#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mqueue.h>
#include <fcntl.h>
#include <sys/stat.h>

// ชื่อ Queue ของ Server
#define SERVER_QUEUE_NAME "/css223_cinema_queue"

// จำนวนที่นั่งทั้งหมดในระบบ
#define MAX_RESOURCES 30

#define MSG_SIZE sizeof(Message)

#define CLIENT_QUEUE_NAME_LEN 32

// กำหนดสิทธิ์การเข้าถึง Message Queue เป็น 0660 (client และ server ต้องใช้ค่าตรงกัน)
#define QUEUE_PERMISSIONS 0660
#define MAX_MESSAGES 10

// Enum กำหนดประเภทคำสั่งที่ Client ส่งหา Server ได้
typedef enum {
    CMD_LIST = 1,
    CMD_STATUS,
    CMD_RESERVE,
    CMD_CANCEL,
    CMD_QUIT
} CommandType;

// Enum กำหนดสถานะผลลัพธ์ที่ Server ใช้ตอบกลับ
typedef enum {
    RES_UNINITIALIZED = 0,
    RES_SUCCESS,
    RES_FAILED,
    RES_INVALID,
    RES_ALREADY_RESERVED
} ResponseStatus;

// Struct สำหรับส่ง-รับข้อความใน POSIX Queue
typedef struct {
    int client_id;
    CommandType cmd;        // คำสั่งที่ส่งไป
    int resource_id;        // เลขที่นั่ง (1-30) ; ใช้ 0 ถ้าไม่เกี่ยวข้อง (เช่น LIST, QUIT)
    ResponseStatus status;  // ผลลัพธ์ตอบกลับ (Server กำหนด)
    char message[256];      // ข้อความแสดงผล (Server กำหนด)
} Message;

// ฟังก์ชัน helper สำหรับสร้างชื่อ Client Queue เฉพาะของแต่ละ client
// เช่น client_id = 3 -> "/client_queue_3"
static inline void get_client_queue_name(int client_id, char *out_buf, size_t buf_size) {
    snprintf(out_buf, buf_size, "/client_queue_%d", client_id);
}

#endif