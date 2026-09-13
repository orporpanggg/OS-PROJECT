# ใช้ Ubuntu LTS เป็นฐาน
FROM ubuntu:22.04

# ปิด prompt โต้ตอบระหว่างติดตั้งแพ็กเกจ
ENV DEBIAN_FRONTEND=noninteractive

# ติดตั้งเครื่องมือคอมไพล์ C/C++, IPC tools และ util พื้นฐาน
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    make \
    util-linux \
    procps \
    nano \
    && rm -rf /var/lib/apt/lists/*

# กำหนดโฟลเดอร์ทำงานภายใน Container
WORKDIR /app

# คัดลอกโค้ดทั้งหมดเข้ามาใน Container
COPY . /app

# สั่ง compile อัตโนมัติเมื่อสร้าง image
# RUN make

# สั่งให้ container ค้างไว้เพื่อให้เปิด terminal เข้ามาทดสอบได้
CMD ["tail", "-f", "/dev/null"]