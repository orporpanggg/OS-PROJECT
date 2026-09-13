#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <mqueue.h>
#include <fcntl.h>
#include <sys/stat.h>

// ==========================================================
//  CSS223 Cinema Reservation Project - Shared Header
//  IPC Mechanism: POSIX Message Queue (mqueue.h)
// ==========================================================

// ชื่อ Queue ของ Server (คงที่ ทุกฝ่ายต้องใช้ชื่อนี้ตรงกัน)
#define SERVER_QUEUE_NAME "/css223_cinema_queue"

// จำนวนที่นั่งทั้งหมดในระบบ
#define MAX_RESOURCES 30

// ขนาดของ Message ที่ใช้ตอน mq_open (mq_msgsize) และ mq_send/mq_receive
#define MSG_SIZE sizeof(Message)

// ความยาวบัฟเฟอร์สำหรับชื่อ client queue เช่น "/client_queue_12345"
#define CLIENT_QUEUE_NAME_LEN 32

// ค่ากำหนดสำหรับการเปิด Message Queue (client และ server ต้องใช้ค่าตรงกัน)
#define QUEUE_PERMISSIONS 0660
#define MAX_MESSAGES 10

// Enum กำหนดประเภทคำสั่งจาก Client
typedef enum {
    CMD_LIST = 1,
    CMD_STATUS,
    CMD_RESERVE,
    CMD_CANCEL,
    CMD_QUIT
} CommandType;

// Enum สถานะตอบกลับจาก Server
typedef enum {
    RES_UNINITIALIZED = 0,
    RES_SUCCESS,
    RES_FAILED,
    RES_INVALID,
    RES_ALREADY_RESERVED    // แยกเคสถูกแย่งจองใน Race Condition ให้ชัดเจน
} ResponseStatus;

// Struct สำหรับส่ง-รับข้อความใน POSIX Queue
// ใช้ struct เดียวกันทั้งขาไป (Client -> Server) และขากลับ (Server -> Client)
typedef struct {
    int client_id;          // หมายเลข Client (1, 2, 3, 4, 5)
    CommandType cmd;        // คำสั่งที่ส่งไป
    int resource_id;        // เลขที่นั่ง (1-30) ; ใช้ 0 ถ้าไม่เกี่ยวข้อง (เช่น LIST, QUIT)
    ResponseStatus status;  // ผลลัพธ์ตอบกลับ (Server เป็นผู้กำหนดค่านี้)
    char message[256];      // ข้อความแสดงผล (Server เป็นผู้กำหนดค่านี้)
} Message;

// ฟังก์ชัน helper สำหรับสร้างชื่อ Client Queue เฉพาะของแต่ละ client
// เช่น client_id = 3 -> "/client_queue_3"
static inline void get_client_queue_name(int client_id, char *out_buf, size_t buf_size) {
    snprintf(out_buf, buf_size, "/client_queue_%d", client_id);
}

#endif